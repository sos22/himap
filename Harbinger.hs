{-# LANGUAGE ScopedTypeVariables #-}
import Data.List
import Control.Monad.Writer.Lazy
import qualified Data.ByteString.Lazy as BSL
import Data.Word
import Data.Char
import System.IO
import System.Random
import Network.BSD
import Data.Time
import Data.Time.Format
import System.Locale
import System.Directory

type Errorable = Writer [String]

runErrorable :: Errorable a -> Either [String] a
runErrorable x = case runWriter x of
  (l, []) -> Right l
  (_, r) -> Left r

data Header = Header String String deriving Show
data Email = Email { eml_headers :: [Header], eml_body :: BSL.ByteString } deriving Show

data Journal = Journal { journal_handle :: Handle,
                         journal_path :: FilePath }
data JournalEntry = JournalStart String
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
  in (createDirectoryIfMissing True journalDir) >> (tryFrom 0)

journalWrite :: Journal -> JournalEntry -> IO ()
journalWrite journal je =
  let h = journal_handle journal in
  case je of
    JournalStart msgId -> hPutStrLn h $ "start " ++ msgId
      

parseAscii7 :: [Word8] -> String
parseAscii7 = map $ chr . fromInteger . toInteger

toSep :: Eq a => a -> [a] -> Maybe ([a], [a])
toSep sep elems =
  let stateSequence elems = zip elems $ zip (inits elems) (tails $ tail elems)
  in case find (\(a, _) -> a == sep) $ stateSequence elems of
    Nothing -> Nothing
    Just x -> Just $ snd x

{- Nothing represents newlines -}
findNewlines :: BSL.ByteString -> [(Maybe Word8, BSL.ByteString)]
findNewlines bsl =
  let tl = BSL.tail bsl
      hd = BSL.head bsl
      hdtl = BSL.head tl
      tltl = BSL.tail tl
  in if BSL.null bsl
     then []
     else if BSL.null tl
          then [(Just hd, BSL.empty)]
          else if or [BSL.null tltl, hd /= 13, hdtl /= 10]
               then (Just hd, tl):(findNewlines tl)
               else (Nothing, tltl):(findNewlines tltl)

lines :: BSL.ByteString -> [([Word8], BSL.ByteString)]
lines s =
  let worker :: [Word8] -> [(Maybe Word8, BSL.ByteString)] -> [([Word8], BSL.ByteString)]
      worker fromPrevNewline remainder =
        case remainder of
          [] -> [(fromPrevNewline, BSL.empty)]
          ((Nothing, trailer):rest) ->
            (fromPrevNewline, trailer):(worker [] rest)
          ((Just c, _):rest) ->
            worker (fromPrevNewline ++ [c]) rest
  in worker [] $ findNewlines s

headersBody :: BSL.ByteString -> Errorable ([String], BSL.ByteString)
headersBody s =
  let ss = Main.lines s in
  case find (\((x,_),_) -> x == []) (zip ss $ inits ss) of
    Nothing -> tell ["Message has no body?"] >> return ( (map (parseAscii7 . fst) ss), BSL.empty)
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

parseEmail :: BSL.ByteString -> Errorable Email
parseEmail what =
  do (headerLines, body) <- headersBody what
     parsedHeaders <- mapM extractHeader $ unfoldHeaderLines headerLines
     return $ Email parsedHeaders body

stringToByteString :: String -> BSL.ByteString
stringToByteString = BSL.pack . map (fromInteger . toInteger . ord)

flattenHeader :: Header -> BSL.ByteString
flattenHeader (Header name value) = BSL.concat $ map stringToByteString [name, ": ", value, "\r\n"]
  
flattenEmail :: Email -> BSL.ByteString
flattenEmail eml =
  BSL.append (BSL.concat $ map flattenHeader (eml_headers eml)) $ BSL.append (stringToByteString "\r\n") $ eml_body eml

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
         hostName <- getHostName
         let newIdent = (show ident) ++ "@" ++ hostName ++ "-harbinger"
         return $ eml {eml_headers = (Header "Message-Id" newIdent):(eml_headers eml)}

addReceivedDate :: Email -> IO (Email, UTCTime)
addReceivedDate eml =
  do now <- Data.Time.getCurrentTime
     let fmtTime = Data.Time.Format.formatTime System.Locale.defaultTimeLocale "%F %T" now
     return (addHeader (Header "X-Harbinger-Received" fmtTime) eml,
             now)

sanitiseForPath :: String -> String
sanitiseForPath = filter $ flip elem "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM,^@%"

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

fileEmail :: Email -> IO ()
fileEmail eml =
  do (eml', when) <- ensureMessageId eml >>= addReceivedDate
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
     journalWrite j (JournalStart $ deMaybe $ getHeader "Message-Id" eml'')
     hh <- openFile poolFile' WriteMode
     BSL.hPut hh $ flattenEmail eml''
     hClose hh
     hClose $ journal_handle j
     removeFile $ journal_path j
     
main :: IO ()
main =
  do parsed <- liftM (runErrorable . parseEmail) BSL.getContents
     case parsed of
       Left errs -> print errs
       Right parsed' -> fileEmail parsed'
     

