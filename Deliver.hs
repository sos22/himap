{-# LANGUAGE ScopedTypeVariables #-}
module Deliver(withStatement,
               fileEmail) where

import Control.Exception.Base
import Control.Monad.Writer.Lazy
import qualified Data.ByteString as BS
import Data.Char
import Data.Int
import Data.List
import qualified Data.Text as DT
import Data.Time
import Data.Time.Format
import qualified Database.SQLite3 as DS
import Debug.Trace
import Network.BSD
import System.Directory
import System.IO
import System.Locale
import System.Posix.Files
import System.Random

import Email
import Util

import ImapServer

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

journalWrite :: Journal -> JournalEntry -> IO ()
journalWrite journal je =
  let h = journal_handle journal in
  hPutStrLn h $ case je of
    JournalStart msgId -> "start " ++ msgId
    JournalAddSymlink msgId name -> "symlink " ++ msgId ++ " " ++ name
    JournalRegisterMessage msgId msgDbId -> "register " ++ msgId ++ " " ++ (show msgDbId)

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

flattenHeader :: Header -> BS.ByteString
flattenHeader (Header name value) = BS.concat $ map stringToByteString [name, ": ", value, "\r\n"]
  
flattenEmail :: Email -> BS.ByteString
flattenEmail eml =
  BS.append (BS.concat $ map flattenHeader (eml_headers eml)) $ BS.append (stringToByteString "\r\n") $ eml_body eml

addDbRow :: DS.Database -> String -> [DS.SQLData] -> IO ()
addDbRow database table values =
  withStatement database (DT.pack $ "INSERT INTO " ++ table ++ " VALUES (" ++ (intercalate ", " (map (const "?") values)) ++ ")") $ \stmt ->
  do DS.bind stmt values
     DS.step stmt
     DS.columns stmt
     return ()
     
addAttribute :: DS.Database -> [(String, DS.SQLData)] -> Int64 -> String -> DS.SQLData -> IO ()
addAttribute database attribs msgId attribName value =
  case lookup attribName attribs of
    Nothing -> error $ "Bad attribute " ++ (show attribName)
    Just attrib -> addDbRow database "MessageAttrs" [DS.SQLInteger msgId, attrib, value]
    
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

transactional :: DS.Database -> IO a -> IO a
transactional database what =
  let end = DS.exec database (DT.pack "END TRANSACTION") in
  do DS.exec database (DT.pack "BEGIN TRANSACTION")
     res <- what `Control.Exception.Base.catch` (\(e::DS.SQLError) -> end >> throw e)
     (end >> return res) `Control.Exception.Base.catch` (\e -> if DS.sqlError e == DS.ErrorBusy
                                                               then transactional database what
                                                               else throw e)
                                                     
withStatement :: DS.Database -> DT.Text -> (DS.Statement -> IO x) -> IO x
withStatement db stmt what =
  do prepped <- DS.prepare db stmt
     (what prepped) `finally` (DS.finalize prepped)

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

emailPoolFile :: Email -> (String, String)
emailPoolFile eml =
  case fmap sanitiseForPath $ getHeader "Message-ID" eml of
    Nothing -> error "message has no ID?"
    Just msgId -> let poolDir = "harbinger/pool/" ++ (take 5 msgId)
                      poolFile = poolDir ++ "/" ++ msgId
                  in (poolDir, poolFile)

addHeader :: Header -> Email -> Email
addHeader hdr eml = eml { eml_headers = hdr:(eml_headers eml) }

getHeader :: String -> Email -> Maybe String
getHeader name eml =
  lookup (map toLower name) $ map (\(Header x y) -> (map toLower x,y)) $ eml_headers eml

getHeaders :: String -> Email -> [String]
getHeaders name eml =
  let name' = map toLower name in
  map (\(Header _ y) -> y) $ filter (\(Header x _) -> name' == (map toLower x)) $ eml_headers eml

removeHeader :: String -> Email -> Email
removeHeader hdr eml = eml { eml_headers = filter (\(Header name _) -> (map toLower name) /= (map toLower hdr)) $ eml_headers eml }

deMaybe :: Maybe a -> a
deMaybe (Just x) = x
deMaybe Nothing = error "Maybe wasn't?"

sanitiseForPath :: String -> String
sanitiseForPath = filter $ flip elem "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM,^@%-+_:"

{- Less a parser and more a canonicaliser.  Maps from strings (usually
   header values) to lists of things under which the message should be indexed. -}
data Parser a = Parser { run_parser :: Bool -> String -> Maybe (a, Bool, String) }

runParser :: Show a => Parser a -> String -> Maybe a
runParser p what =
  case run_parser p False what of
    Just (x, _, "") -> trace ("Parsing " ++ what ++ " gave " ++ (show x)) $
                       Just x
    Just (_, _, leftovers) -> trace ("Junk at end of field? " ++ (show leftovers)) Nothing
    Nothing -> Nothing

instance Monad Parser where
  return x = Parser $ \c r -> Just (x, c, r)
  fstM >>= sndF = Parser $ \c initialStr ->
    case run_parser fstM c initialStr of
      Nothing -> Nothing
      Just (res1, c', midStr) -> run_parser (sndF res1) c' midStr
  
parseAlternatives :: [Parser a] -> Parser a
parseAlternatives [] = Parser $ \_ _ -> Nothing
parseAlternatives (a:as) = Parser $ \c str ->
  case run_parser a c str of
    Nothing -> run_parser (parseAlternatives as) c str
    Just (res, c', leftover) -> Just (res, c', leftover)

parseNonEmpty :: String -> Parser a -> Parser a
parseNonEmpty msg parser =
  Parser $ \_ initStr ->
  case run_parser parser False initStr of
    Nothing -> Nothing
    Just (_, False, "") -> Nothing
    Just (_, False, leftover) -> trace (msg ++ " failed to consume any input at " ++ (show initStr) ++ ", leftover " ++ (show leftover))
                                 Nothing
    Just (res, True, leftover) -> Just (res, True, leftover)
    
parseMany1 :: Parser a -> Parser [a]
parseMany1 p =
  do one <- parseNonEmpty "many1" p
     rest <- parseAlternatives [parseMany1 p, return []]
     return $ one:rest
parseMany :: Parser a -> Parser [a]
parseMany p = parseAlternatives [parseMany1 p, return []]
parseMany1Sep :: Parser a -> Parser b -> Parser [a]
parseMany1Sep thing seperator =
  parseAlternatives [do one <- parseNonEmpty "many1sep" (do x <- thing
                                                            _ <- seperator
                                                            return x)
                        rest <- parseMany1Sep thing seperator
                        return $ one:rest,
                     do one <- parseNonEmpty "many1sep singleton" thing
                        return [one]]
parseOptional :: Parser a -> Parser (Maybe a)
parseOptional what = parseAlternatives [ liftM Just what,
                                         return Nothing ]
parseChar :: Char -> Parser Char
parseChar c = Parser $ \_ x -> case x of
  "" -> Nothing
  (cc:ccs) | cc == c -> Just (c, True, ccs)
           | otherwise -> Nothing
parseCharRange :: Int -> Int -> Parser Char
parseCharRange start end = Parser $ \_ x ->
  case x of
    [] -> Nothing
    xx:xs ->
      if ord xx >= start && ord xx <= end
      then Just (xx, True, xs)
      else Nothing

fileEmail :: String -> [MessageFlag] -> DS.Database -> [(String,DS.SQLData)] -> Email -> IO ()
fileEmail mailbox flags database attribs eml =
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
     let addAttrib :: String -> String -> IO ()
         addAttrib name val =
           addAttribute database attribs msgDbId name $ DS.SQLText $ DT.pack val
         addAttribs :: String -> String -> Parser [String] -> IO ()
         addAttribs name header parser =
           mapM_ (addAttrib name) $ concatMap (maybe [] id . runParser parser) $ getHeaders header eml''
         parseMsgId :: Parser String
         parseMsgId = do _ <- parseOptional $ parseCFWS
                         _ <- parseChar '<'
                         l <- parseIdLeft
                         _ <- parseChar '@'
                         r <- parseIdRight
                         _ <- parseChar '>'
                         _ <- parseOptional $ parseCFWS
                         return $ l ++ "@" ++ r
         parseIdLeft :: Parser String
         parseIdLeft = parseAlternatives [parseDotAtomText, parseNoFoldQuote, parseObsIdLeft]
         parseIdRight :: Parser String
         parseIdRight = parseAlternatives [parseDotAtomText, parseNoFoldLiteral, parseObsIdRight]
         parseNoFoldQuote :: Parser String
         parseNoFoldQuote = do _ <- parseChar '"'
                               _ <- parseOptional $ parseChar '\''
                               r <- liftM concat $ parseMany (parseAlternatives [liftM singleton parseQtext, parseQuotedPair])
                               _ <- parseOptional $ parseChar '\''
                               _ <- parseChar '"'
                               return r
         parseNoFoldLiteral :: Parser String
         parseNoFoldLiteral = do _ <- parseChar '['
                                 r <- liftM concat $ parseMany (parseAlternatives [liftM singleton parseDtext, parseQuotedPair])
                                 _ <- parseChar ']'
                                 return r
         parseMailboxList :: Parser [String]
         parseMailboxList = parseAlternatives [liftM concat $ parseMany1Sep parseMailbox (parseChar ','),
                                               parseObsMboxList]
         parseAddressList :: Parser [String]
         parseAddressList = parseAlternatives [liftM concat $ parseMany1Sep parseAddress (parseChar ','),
                                               parseObsAddrList]
         parseUnstructured :: Parser [String]
         parseUnstructured = do r <- parseMany $ do a <- parseOptional parseFWS
                                                    b <- parseUtext
                                                    return $ case a of
                                                      Nothing -> b
                                                      Just _ -> ' ':b
                                _ <- parseOptional parseFWS
                                return [concat r]
         parseUtext :: Parser String
         parseUtext = parseAlternatives [liftM (\x -> [x]) parseNoWsCtl,
                                         liftM (\x -> [x]) $ parseCharRange 33 126,
                                         parseObsUtext]
         parsePhrase :: Parser String
         parsePhrase = parseAlternatives [liftM (intercalate " ") $ parseMany1 parseWord, parseObsPhrase]
         parseWord :: Parser String
         parseWord = parseAlternatives [parseAtom, parseQuotedString]
         parseAddress :: Parser [String]
         parseAddress = parseAlternatives [parseMailbox, parseGroup]
         parseMailbox :: Parser [String]
         parseMailbox = parseAlternatives [parseNameAddr, liftM (\x -> [x]) parseAddrSpec]
         parseNameAddr :: Parser [String]
         parseNameAddr = do dn <- parseOptional parseDisplayName
                            aa <- parseAngleAddr
                            return $ case dn of
                              Just dn' -> [dn',aa]
                              Nothing -> [aa]
         parseAngleAddr :: Parser String
         parseAngleAddr = parseAlternatives [do _ <- parseOptional parseCFWS
                                                as <- do _ <- parseChar '<'
                                                         r <- parseAddrSpec
                                                         _ <- parseChar '>'
                                                         return r
                                                _ <- parseOptional parseCFWS
                                                return as,
                                             parseObsAngleAddr]
         parseGroup :: Parser [String]
         parseGroup = do dn <- parseDisplayName
                         _ <- parseChar ':'
                         ml <- parseOptional $ parseAlternatives [parseMailboxList, parseCFWS >> return []]
                         _ <- parseChar ';'
                         _ <- parseOptional parseCFWS
                         return $ case ml of
                           Just x -> dn:x
                           Nothing -> [dn]
         parseDisplayName :: Parser String
         parseDisplayName = parsePhrase
         parseAddrSpec :: Parser String
         parseAddrSpec = do l <- parseLocalPart
                            _ <- parseChar '@'
                            d <- parseDomain
                            return $ l ++ "@" ++ d
         parseLocalPart :: Parser String
         parseLocalPart = parseAlternatives [parseDotAtom, parseQuotedString, parseObsLocalPart]
         parseDomain :: Parser String
         parseDomain = parseAlternatives [parseDotAtom, parseDomainLiteral, parseObsDomain]
         parseDomainLiteral :: Parser String
         parseDomainLiteral = do _ <- parseOptional parseCFWS
                                 _ <- parseChar '['
                                 r <- parseMany $ do a <- parseOptional parseFWS
                                                     r <- parseDcontent
                                                     return $ case a of
                                                       Nothing -> r
                                                       Just _ -> ' ':r
                                 _ <- parseOptional parseFWS
                                 _ <- parseChar ']'
                                 _ <- parseOptional parseCFWS
                                 return $ concat r
         parseDcontent :: Parser String
         parseDcontent = parseAlternatives [liftM singleton parseDtext, parseQuotedPair]
         parseDtext :: Parser Char
         parseDtext = parseAlternatives [parseNoWsCtl, parseCharRange 33 90, parseCharRange 94 126]
         parseNoWsCtl :: Parser Char
         parseNoWsCtl = parseAlternatives [parseCharRange 1 8,
                                           parseCharRange 11 12,
                                           parseCharRange 14 31,
                                           parseCharRange 127 127]
         singleton x = [x]
         parseText :: Parser String
         parseText = parseAlternatives [liftM singleton $ parseCharRange 1 9,
                                        liftM singleton $ parseCharRange 11 12,
                                        liftM singleton $ parseCharRange 14 127,
                                        parseObsText]
         parseQuotedPair :: Parser String
         parseQuotedPair = parseAlternatives [(parseChar '\\') >> parseText,
                                              liftM singleton parseObsQp]
         parseFWS :: Parser ()
         parseFWS = parseAlternatives [parseMany1 parseWSP >> return (), parseObsFWS] >> return ()
         parseCFWS :: Parser ()
         parseCFWS = do _ <- parseMany (parseOptional parseFWS >> parseComment)
                        parseAlternatives [(parseOptional parseFWS) >> parseComment,
                                           parseFWS]
                        return ()
         parseComment :: Parser ()
         parseComment = do _ <- parseChar '('
                           _ <- parseMany (parseFWS >> parseCContent)
                           parseFWS
                           _ <- parseChar ')'
                           return ()
         parseCContent :: Parser ()
         parseCContent = parseAlternatives [parseCText, parseQuotedPair >> return (), parseComment] >> return ()
         parseCText = parseAlternatives [parseNoWsCtl,
                                         parseCharRange 33 39,
                                         parseCharRange 41 91,
                                         parseCharRange 93 126] >> return ()
         parseWSP = parseAlternatives $ map parseChar " \r\t\n\v"
         parseDotAtom :: Parser String
         parseDotAtom = do _ <- parseOptional parseCFWS
                           r <- parseDotAtomText
                           _ <- parseOptional parseCFWS
                           return r
         parseDotAtomText :: Parser String
         parseDotAtomText = liftM (intercalate ".") $ parseMany1Sep (parseMany1 parseAText) (parseChar '.')
         parseAText = parseAlternatives $ map parseChar "1234567890qwertyuiopasdfghklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM!#$%&\'*+-/=?`{|}~"
         parseAtom :: Parser String
         parseAtom = do _ <- parseOptional parseCFWS
                        r <- parseMany1 parseAText
                        _ <- parseOptional parseCFWS
                        return r
         parseQuotedString :: Parser String
         parseQuotedString = do _ <- parseOptional parseCFWS
                                _ <- parseChar '"'
                                r <- parseMany $ do a <- parseOptional parseFWS
                                                    b <- parseQcontent
                                                    return $ case a of
                                                      Nothing -> b
                                                      Just _ -> ' ':b
                                _ <- parseChar '"'
                                _ <- parseOptional parseCFWS
                                return $ concat r
         parseQcontent :: Parser String
         parseQcontent = parseAlternatives [liftM singleton parseQtext, parseQuotedPair]
         parseQtext :: Parser Char
         parseQtext = parseAlternatives [parseNoWsCtl,
                                         parseCharRange 33 33,
                                         parseCharRange 35 91,
                                         parseCharRange 93 126]
         parseObsIdLeft = parseLocalPart
         parseObsIdRight = parseDomain
         parseObsMboxList :: Parser [String]
         parseObsMboxList =
           do r <- liftM concat $ parseMany1 $ do rr <- parseOptional parseMailbox
                                                  _ <- parseOptional parseCFWS
                                                  _ <- parseChar ','
                                                  _ <- parseOptional parseCFWS
                                                  return $ maybe [] id rr
              l <- parseOptional parseMailbox
              return $ case l of
                Nothing -> r
                Just ls -> ls ++ r
         parseObsAddrList :: Parser [String]
         parseObsAddrList = do r <- parseMany1 $ do rr <- parseOptional parseAddress
                                                    _ <- parseOptional parseCFWS
                                                    _ <- parseChar ','
                                                    _ <- parseOptional parseCFWS
                                                    return $ maybe [] id rr
                               l <- parseOptional parseAddress
                               return $ (concat r) ++ (maybe [] id l)
         parseObsAngleAddr :: Parser String
         parseObsAngleAddr = do _ <- parseOptional parseCFWS
                                _ <- parseChar '<'
                                _ <- parseOptional parseObsRoute
                                a <- parseAddrSpec
                                _ <- parseChar '>'
                                _ <- parseOptional parseCFWS
                                return a
         parseObsRoute = do _ <- parseOptional parseCFWS
                            dl <- parseObsDomainList
                            _ <- parseChar ':'
                            _ <- parseOptional parseCFWS
                            return dl
         parseObsDomainList :: Parser String
         parseObsDomainList = do _ <- parseChar '@'
                                 d <- parseDomain
                                 ds <- parseMany $ do _ <- parseMany $ parseAlternatives [parseCFWS, parseChar ',' >> return ()]
                                                      _ <- parseOptional parseCFWS
                                                      _ <- parseChar '@'
                                                      parseDomain
                                 return $ '@':(intercalate "@" (d:ds))
         parseObsDomain :: Parser String
         parseObsDomain = liftM (intercalate ".") $ parseMany1Sep parseAtom (parseChar '.')
         parseObsLocalPart :: Parser String
         parseObsLocalPart = liftM (intercalate ".") $ parseMany1Sep parseWord (parseChar '.')
         parseObsUtext :: Parser String
         parseObsUtext = parseObsText
         parseObsText :: Parser String
         parseObsText = do _ <- parseMany $ parseChar '\r'
                           _ <- parseMany $ parseChar '\n'
                           liftM concat $ parseMany $ do rr <- parseObsChar
                                                         a <- parseMany $ parseChar '\r'
                                                         b <- parseMany $ parseChar '\n'
                                                         return $ case (a, b) of
                                                           ("", "") -> [rr]
                                                           _ -> rr:" "
         parseObsChar :: Parser Char
         parseObsChar = parseAlternatives [ parseCharRange 0 9,
                                            parseCharRange 11 12,
                                            parseCharRange 14 127]
         parseObsPhrase :: Parser String
         parseObsPhrase = do w <- parseWord
                             r <- parseMany $ parseAlternatives [parseWord,
                                                                 liftM singleton $ parseChar '.',
                                                                 parseCFWS >> return ""]
                             return $ intercalate " " $ w:r
         parseObsQp = (parseChar '\\') >> parseCharRange 0 127
         parseObsFWS :: Parser ()
         parseObsFWS = (parseMany1 parseWSP) >> (parseMany (parseChar '\r' >> parseChar '\n' >> parseMany1 parseWSP)) >> return ()
     addAttribs "rfc822.In-Reply-To" "in-reply-to" $ parseMany1 parseMsgId
     addAttribs "rfc822.References" "references" $ parseMany1 parseMsgId
     addAttribs "rfc822.From" "from" parseMailboxList
     addAttribs "rfc822.Sender" "sender" parseMailbox
     addAttribs "rfc822.Reply-To" "reply-to" parseAddressList
     addAttribs "rfc822.To" "to" parseAddressList
     addAttribs "rfc822.Cc" "cc" parseAddressList
     addAttribs "rfc822.Bcc" "bcc" parseAddressList
     addAttribs "rfc822.Subject" "subject" parseUnstructured
     addAttribs "rfc822.Comments" "comments" parseUnstructured
     addAttribs "rfc822.Keywords" "keywords" $ parseMany1Sep parsePhrase (parseChar ',')
     addAttribs "rfc822.Date" "date" parseUnstructured
     flip mapM_ (MessageFlagRecent:flags) $ \flag ->
       addAttribute database attribs msgDbId (msgFlagDbName flag) (DS.SQLInteger 1)
     addAttribute database attribs msgDbId "harbinger.mailbox" (DS.SQLText $ DT.pack mailbox)
     hClose $ journal_handle j
     removeFile $ journal_path j

