{-# LANGUAGE ScopedTypeVariables #-}
import Data.List
import Control.Monad.Writer.Lazy
import qualified Data.ByteString as BS
import Data.Word
import Data.Char
import System.IO
import System.Random
import Network.BSD
import Data.Time
import Data.Time.Format
import System.Locale
import System.Directory
import System.Posix.Files
import qualified Database.SQLite3 as DS
import qualified Data.Text as DT
import Control.Exception.Base
import Data.Int

import Email
import Util

type Errorable = Writer [String]

runErrorable :: Errorable a -> Either [String] a
runErrorable x = case runWriter x of
  (l, []) -> Right l
  (_, r) -> Left r

data Journal = Journal { journal_handle :: Handle,
                         journal_path :: FilePath }
data JournalEntry = JournalStart String
                  | JournalAddSymlink String String
                  | JournalRegisterMessage String Int64
                    deriving Show

openCreate :: FilePath -> IO (Maybe Handle)
openCreate path = do exists <- doesFileExist path
                     if exists
                       then return Nothing
                       else liftM Just $ openFile path WriteMode

openMessageJournal :: IO Journal
openMessageJournal =
  let journalDir = "harbinger/journal"
      journalPath cntr = journalDir ++ "/" ++ (show cntr)
      tryFrom cntr = let p = journalPath cntr
                     in do ee <- openCreate p
                           case ee of
                             Nothing -> tryFrom $ cntr + 1
                             Just hh -> return $ Journal { journal_handle = hh, journal_path = p }
  in (createDirectoryIfMissing True journalDir) >> (tryFrom (0::Int))

journalWrite :: Journal -> JournalEntry -> IO ()
journalWrite journal je =
  let h = journal_handle journal in
  hPutStrLn h $ case je of
    JournalStart msgId -> "start " ++ msgId
    JournalAddSymlink msgId name -> "symlink " ++ msgId ++ " " ++ name
    JournalRegisterMessage msgId msgDbId -> "register " ++ msgId ++ " " ++ (show msgDbId)

parseAscii7 :: [Word8] -> String
parseAscii7 = map $ chr . fromInteger . toInteger

toSep :: Eq a => a -> [a] -> Maybe ([a], [a])
toSep sep elems =
  let stateSequence = zip elems $ zip (inits elems) (tails $ tail elems)
  in case find (\(a, _) -> a == sep) stateSequence of
    Nothing -> Nothing
    Just x -> Just $ snd x

{- Nothing represents newlines -}
findNewlines :: BS.ByteString -> [(Maybe Word8, BS.ByteString)]
findNewlines bsl =
  let tl = BS.tail bsl
      hd = BS.head bsl
      hdtl = BS.head tl
      tltl = BS.tail tl
  in if BS.null bsl
     then []
     else if BS.null tl
          then [(Just hd, BS.empty)]
          else if or [BS.null tltl, hd /= 13, hdtl /= 10]
               then (Just hd, tl):(findNewlines tl)
               else (Nothing, tltl):(findNewlines tltl)

lines :: BS.ByteString -> [([Word8], BS.ByteString)]
lines s =
  let worker :: [Word8] -> [(Maybe Word8, BS.ByteString)] -> [([Word8], BS.ByteString)]
      worker fromPrevNewline remainder =
        case remainder of
          [] -> [(fromPrevNewline, BS.empty)]
          ((Nothing, trailer):rest) ->
            (fromPrevNewline, trailer):(worker [] rest)
          ((Just c, _):rest) ->
            worker (fromPrevNewline ++ [c]) rest
  in worker [] $ findNewlines s

headersBody :: BS.ByteString -> Errorable ([String], BS.ByteString)
headersBody s =
  let ss = Main.lines s in
  case find (\((x,_),_) -> x == []) (zip ss $ inits ss) of
    Nothing -> tell ["Message has no body?"] >> return ( (map (parseAscii7 . fst) ss), BS.empty)
    Just ((_blank, body), headers) -> return (map (parseAscii7 . fst) headers, body) 

unfoldHeaderLines :: [String] -> [String]
unfoldHeaderLines what =
  let isLinearWhiteSpace c = c `elem` " \t"
      worker :: [String] -> (Maybe String, [String])
      worker [] = (Nothing, [])
      worker ([]:others) = case worker others of
        (Nothing, res) -> (Nothing, res)
        (Just l, res) -> (Nothing, l:res)
      worker (ccs@(c:_):others) | isLinearWhiteSpace c =
        case worker others of
          (Nothing, res) -> (Just ccs, res)
          (Just r, res) -> (Just (ccs ++ r), res)
        | otherwise = case worker others of
          (Nothing, res) -> (Nothing, ccs:res)
          (Just e, res) -> (Nothing, (ccs ++ e):res)
  in case worker what of
    (Nothing, res) -> res
    (Just c, res) -> c:res

extractHeader :: String -> Errorable Header
extractHeader hdrLine =
  case toSep ':' hdrLine of
    Nothing -> tell ["No : in header line " ++ hdrLine] >> (return $ Header hdrLine "")
    Just (a, b) -> return $ Header a (dropWhile (flip elem " \t") b)

parseEmail :: BS.ByteString -> Errorable Email
parseEmail what =
  do (headerLines, body) <- headersBody what
     parsedHeaders <- mapM extractHeader $ unfoldHeaderLines headerLines
     return $ Email parsedHeaders body

flattenHeader :: Header -> BS.ByteString
flattenHeader (Header name value) = BS.concat $ map stringToByteString [name, ": ", value, "\r\n"]
  
flattenEmail :: Email -> BS.ByteString
flattenEmail eml =
  BS.append (BS.concat $ map flattenHeader (eml_headers eml)) $ BS.append (stringToByteString "\r\n") $ eml_body eml

getHeader :: String -> Email -> Maybe String
getHeader name eml =
  lookup (map toLower name) $ map (\(Header x y) -> (map toLower x,y)) $ eml_headers eml

addHeader :: Header -> Email -> Email
addHeader hdr eml = eml { eml_headers = hdr:(eml_headers eml) }

removeHeader :: String -> Email -> Email
removeHeader hdr eml = eml { eml_headers = filter (\(Header name _) -> (map toLower name) /= (map toLower hdr)) $ eml_headers eml }

ensureMessageId :: Email -> IO Email
ensureMessageId eml =
  case getHeader "Message-Id" eml of
    Just _ -> return eml
    Nothing ->
      do {- Pick a range big enough to be statistically unique -}
         ident <- getStdRandom $ randomR (0::Integer,1000000000000000)
         hostname <- getHostName
         let newIdent = (show ident) ++ "@" ++ hostname ++ "-harbinger"
         return $ eml {eml_headers = (Header "Message-Id" newIdent):(eml_headers eml)}

addReceivedDate :: Email -> IO (Email, UTCTime)
addReceivedDate eml =
  do now <- Data.Time.getCurrentTime
     let fmtTime = Data.Time.Format.formatTime System.Locale.defaultTimeLocale "%F %T" now
     return (addHeader (Header "X-Harbinger-Received" fmtTime) eml,
             now)

sanitiseForPath :: String -> String
sanitiseForPath = filter $ flip elem "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM,^@%-+_:"

emailPoolFile :: Email -> (String, String)
emailPoolFile eml =
  case fmap sanitiseForPath $ getHeader "Message-ID" eml of
    Nothing -> error "message has no ID?"
    Just msgId -> let poolDir = "harbinger/pool/" ++ (take 5 msgId)
                      poolFile = poolDir ++ "/" ++ msgId
                  in (poolDir, poolFile)

deMaybe :: Maybe a -> a
deMaybe (Just x) = x
deMaybe Nothing = error "Maybe wasn't?"

addDbRow :: DS.Database -> String -> [DS.SQLData] -> IO ()
addDbRow database table values =
  withStatement database (DT.pack $ "INSERT INTO " ++ table ++ " VALUES (" ++ (intercalate ", " (map (const "?") values)) ++ ")") $ \stmt ->
  do DS.bind stmt values
     DS.step stmt
     DS.columns stmt >>= print
     
addAttribute :: DS.Database -> [(String, Int64)] -> Int64 -> String -> DS.SQLData -> IO ()
addAttribute database attribs msgId attribName value =
  case lookup attribName attribs of
    Nothing -> error $ "Bad attribute " ++ (show attribName)
    Just attrib -> addDbRow database "MessageAttrs" [DS.SQLInteger msgId, DS.SQLInteger attrib, value]
    
transactional :: DS.Database -> IO a -> IO a
transactional database what =
  let end = DS.exec database (DT.pack "END TRANSACTION") in
  do DS.exec database (DT.pack "BEGIN TRANSACTION")
     res <- what `Control.Exception.Base.catch` (\(e::DS.SQLError) -> end >> throw e)
     (end >> return res) `Control.Exception.Base.catch` (\e -> if DS.sqlError e == DS.ErrorBusy
                                                               then transactional database what
                                                               else throw e)
                                                     
allocMsgDbId :: DS.Database -> IO Int64
allocMsgDbId database =
  transactional database $
  withStatement database (DT.pack "SELECT Val FROM NextMessageId") $ \stmt ->
  do r <- DS.step stmt
     case r of
       DS.Row -> do v <- DS.columns stmt
                    case v of
                      [DS.SQLInteger nextId] ->
                        withStatement database (DT.pack "UPDATE NextMessageId SET Val = ?") $ \stmt' ->
                        do DS.bindInt64 stmt' (DS.ParamIndex 1) (nextId + 1)
                           DS.step stmt'
                           DS.step stmt'
                           return nextId
                      _ -> error $ "NextMessageId table contained unexpected value " ++ (show v)
       _ -> error $ "Cannot query NextMessageId table"

fileEmail :: DS.Database -> [(String,Int64)] -> Email -> IO ()
fileEmail database attribs eml =
  do (eml', receivedAt) <- ensureMessageId eml >>= addReceivedDate
     let (_, poolFile) = emailPoolFile eml'
     conflict <- doesFileExist poolFile
     eml'' <- if conflict
              then ensureMessageId $
                   addHeader (Header "X-Harbinger-Old-Id" $ deMaybe $ getHeader "Message-Id" eml') $
                   removeHeader "Message-Id" eml'
              else return eml'
     let (poolDir', poolFile') = emailPoolFile eml''
     conflict' <- doesFileExist poolFile'
     if conflict'
       then error $ "Failed to generate unique message ID! (" ++ poolFile ++ ", " ++ poolFile' ++ ")"
       else return ()
     createDirectoryIfMissing True poolDir'
     j <- openMessageJournal
     let msgId = deMaybe $ getHeader "Message-Id" eml''
     journalWrite j $ JournalStart msgId
     hh <- openFile poolFile' WriteMode
     BS.hPut hh $ flattenEmail eml''
     hClose hh
     let dateSymlinkDir = Data.Time.Format.formatTime System.Locale.defaultTimeLocale "harbinger/byDate/%F/" receivedAt
         dateSymlinkPath = dateSymlinkDir ++ (sanitiseForPath $ deMaybe $ getHeader "Message-Id" eml'')
     createDirectoryIfMissing True dateSymlinkDir
     journalWrite j $ JournalAddSymlink msgId dateSymlinkPath
     createSymbolicLink ("../../../" ++ poolFile') dateSymlinkPath
     msgDbId <- allocMsgDbId database
     journalWrite j $ JournalRegisterMessage msgId msgDbId
     addDbRow database "Messages" [DS.SQLInteger msgDbId, DS.SQLText $ DT.pack poolFile']
     addAttribute database attribs msgDbId "rfc822.Message-Id" (DS.SQLText $ DT.pack msgId)
     addAttribute database attribs msgDbId "harbinger.seen" (DS.SQLInteger 0)
     addAttribute database attribs msgDbId "harbinger.recent" (DS.SQLInteger 1)
     addAttribute database attribs msgDbId "harbinger.mailbox" (DS.SQLText $ DT.pack "INBOX")
     hClose $ journal_handle j
     removeFile $ journal_path j

withStatement :: DS.Database -> DT.Text -> (DS.Statement -> IO x) -> IO x
withStatement db stmt what =
  do prepped <- DS.prepare db stmt
     (what prepped) `finally` (DS.finalize prepped)

runStatement :: DS.Statement -> IO [[DS.SQLData]]
runStatement s = do r <- DS.step s
                    case r of
                      DS.Done -> return []
                      DS.Row -> (DS.columns s) >>=
                                (flip liftM (runStatement s) . (:))
                                
dbQuery :: DS.Database -> DT.Text -> IO [[DS.SQLData]]                                      
dbQuery db query = withStatement db query runStatement 

{- Our database schema is a massive abuse.  All of the data gets
jammed into one table, keyed off of (messageID, attributeID), with a
single non-key field containing the value of the attribute.  We then
rely entirely on higher-level correctness to make sure that the
content of the database is vaguely sane, with no DB-level constraints
at all.  The main reason for that is that I want to be able to add
more attributes later on without having to do a schema upgrade. -}
initialiseDatabase :: DS.Database -> IO ()
initialiseDatabase db =
  let stmts = map (DS.exec db . DT.pack)
              ["BEGIN TRANSACTION",
               "CREATE TABLE NextMessageId (Val INTEGER)",
               "INSERT INTO NextMessageId (Val) VALUES (1)",
               "CREATE TABLE Messages (MessageId INTEGER PRIMARY KEY NOT NULL, Location TEXT NOT NULL)",
               "CREATE TABLE Attributes (AttributeId INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, Description TEXT NOT NULL)",
               "CREATE TABLE MessageAttrs (MessageId REFERENCES Messages(MessageId) NOT NULL, AttributeId REFERENCES Attributes(AttributeId) NOT NULL, Value NOT NULL)",
               "CREATE UNIQUE INDEX AttributeRmap ON Attributes (Description)",
               "CREATE INDEX AttrRmap ON MessageAttrs (AttributeId, Value)",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Message-Id')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.seen')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.mailbox')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.recent')",
               "CREATE TABLE HarbingerVersion (Version INTEGER)",
               "INSERT INTO HarbingerVersion (Version) VALUES (1)",
               "END TRANSACTION"]
  in sequence_ stmts
     
loadAttributeTable :: DS.Database -> IO [(String, Int64)]
loadAttributeTable database =
  withStatement database (DT.pack "SELECT * FROM Attributes") worker
  where worker stmt = do r <- DS.step stmt
                         case r of
                           DS.Row ->
                             do v <- DS.columns stmt
                                case v of
                                  [DS.SQLInteger attribId, DS.SQLText description] ->
                                    liftM ((:) (DT.unpack description, attribId)) $ worker stmt
                                  _ -> error $ "Unexpected entry " ++ (show v) ++ " in message attributes table"
                           DS.Done -> return []
                           
main :: IO ()
main =
  do database <- DS.open $ DT.pack "harbinger.db"
     version <-
       catchJust
       (\exception -> if DS.sqlError exception == DS.ErrorError
                      then Just ()
                      else Nothing)
       (do versions <- dbQuery database $ DT.pack "SELECT Version FROM HarbingerVersion"
           case versions of
             [] -> error "version table exists but is empty?"
             [[DS.SQLInteger n]] | n > 0 -> return n
             (_:_:_) -> error $ "version table contains multiple entries? " ++ (show versions)
             [x] -> error $ "version number is not a positive integer? " ++ (show x))
       (\() -> return 0)
     case version of
       0 -> initialiseDatabase database
       1 -> return ()
       _ -> error $ "Database is in version " ++ (show version) ++ ", but we only support version 1"
     attribs <- loadAttributeTable database
     print attribs
     parsed <- liftM (runErrorable . parseEmail) BS.getContents
     case parsed of
       Left errs -> print errs
       Right parsed' -> fileEmail database attribs parsed'

