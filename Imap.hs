{-# LANGUAGE GeneralizedNewtypeDeriving #-}
import Network
import qualified Data.ByteString as BS
import System.IO
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Char
import Data.IORef
import Data.List
import Data.Word
import Debug.Trace
import qualified Database.SQLite3 as DS
import qualified Data.Text as DT

ignore :: Monad m => m a -> m ()
ignore = flip (>>) $ return ()

stringToByteString :: String -> BS.ByteString
stringToByteString = BS.pack . map (fromInteger.toInteger.ord)
byteStringToString :: BS.ByteString -> String
byteStringToString = map (chr.fromInteger.toInteger) . BS.unpack

data ImapServerState = ImapServerState { iss_handle :: Handle, 
                                         iss_outgoing_response :: [BS.ByteString],
                                         iss_inbuf :: IORef BS.ByteString,
                                         iss_inbuf_idx :: Int,
                                         iss_uids :: [MsgUid],
                                         iss_database :: DS.Database,
                                         iss_attributes :: [(String, DS.SQLData)]
                                         }

data ImapStopReason = ImapStopFinished
                    | ImapStopFailed String
                    | ImapStopBacktrack
                      deriving Show
data ImapServer a = ImapServer { run_is :: ImapServerState -> IO (Either ImapStopReason (ImapServerState, a)) }
instance Monad ImapServer where
  return x = ImapServer $ \state -> return $ Right (state, x)
  first >>= secondF = ImapServer $ \state ->
    do firstRes <- run_is first state
       case firstRes of
         Left r -> return $ Left r
         Right (newState, firstRes') ->
           run_is (secondF firstRes') newState

loadAttributes :: ImapServer ()
loadAttributes =
  do attribs <- dbQuery "SELECT AttributeId, Description FROM Attributes" []
     let attribs' = flip map attribs $ \r ->
           case r of
             [attribId, DS.SQLText description] -> (DT.unpack description, attribId)
             _ -> error $ "Bad attribute definition " ++ (show r)
     ImapServer $ \state -> return $ Right (state {iss_attributes = attribs'}, ())
     
runImapServer :: Handle -> ImapServer () -> IO ()
runImapServer hndle is =
  do inbuf <- newIORef BS.empty
     database <- DS.open $ DT.pack "harbinger.db"
     ignore $ run_is (loadAttributes >> is) $
       ImapServerState { iss_handle = hndle, 
                         iss_outgoing_response = [],
                         iss_inbuf = inbuf, 
                         iss_inbuf_idx = 0,
                         iss_uids = [], 
                         iss_database = database, 
                         iss_attributes = error "failed to load attributes DB?"}

queueResponse :: String -> ImapServer ()
queueResponse what = ImapServer $ \isState ->
  return $ Right (isState {iss_outgoing_response = (stringToByteString what):
                                                   (iss_outgoing_response isState)},
                  ())

finishResponse :: ImapServer ()
finishResponse = ImapServer $ \isState ->
  let worker [] = return ()
      worker (x:xs) = worker xs >> ((trace $ "sending " ++ (byteStringToString x)) $ BS.hPut (iss_handle isState) x)
  in do worker (iss_outgoing_response isState)
        hFlush (iss_handle isState)
        return $ Right (isState {iss_outgoing_response = []}, ())

data ResponseTag = ResponseUntagged
                 | ResponseTagged String
                   deriving Show
data ResponseState = ResponseStateOk
                   | ResponseStateNo
                   | ResponseStateBad
                   | ResponseStateNone
                     deriving Show
data ResponseAttribute = ResponseAttribute String
                         deriving Show
attrToString :: ResponseAttribute -> String
attrToString (ResponseAttribute x) = x

data ByteRange = ByteRange Int Int
               deriving Show
data MimeSectionPath = MimeSectionPath [Int]
                     deriving Show
data SectionSpec = SectionMsgText
                 | SectionMsgHeaderFields Bool [String]
                 | SectionMsgHeader
                 | SectionMsgMime
                 | SectionSubPart MimeSectionPath (Maybe SectionSpec)
                   deriving Show
data MsgSequenceNumber = MsgSequenceNumber Int
                         deriving (Show)
instance Enum MsgSequenceNumber where
  toEnum = MsgSequenceNumber
  fromEnum (MsgSequenceNumber x) = x
  
data FetchAttribute = FetchAttrBody Bool (Maybe SectionSpec) (Maybe ByteRange)
                    | FetchAttrBodyStructure
                    | FetchAttrEnvelope
                    | FetchAttrFlags
                    | FetchAttrInternalDate
                    | FetchAttrRfc822
                    | FetchAttrRfc822Size
                    | FetchAttrRfc822Header
                    | FetchAttrRfc822Text
                    | FetchAttrUid
                      deriving Show
                               
newtype MsgUid = MsgUid Int
               deriving (Show, Enum, Ord, Eq)
                     
data ImapCommand = ImapNoop
                 | ImapCapability
                 | ImapLogin String String
                 | ImapList String String
                 | ImapSelect String
                 | ImapFetch [MsgSequenceNumber] [FetchAttribute]
                 | ImapFetchUid [MsgUid] [FetchAttribute]
                 | ImapCommandBad String
                   deriving Show
     
sendResponse :: ResponseTag -> ResponseState -> [ResponseAttribute] -> String -> ImapServer ()
sendResponse tag state attrs resp =
  do case tag of
       ResponseUntagged -> queueResponse "*"
       ResponseTagged tag' -> queueResponse tag'
     queueResponse " "
     case state of
       ResponseStateOk -> queueResponse "OK "
       ResponseStateNo -> queueResponse "NO "
       ResponseStateBad -> queueResponse "BAD "
       ResponseStateNone -> return ()
     case map attrToString attrs of
       [] -> return ()
       attrs' -> let w [] = queueResponse "]"
                     w [x] = queueResponse x >> w []
                     w (x:xs) = queueResponse x >> queueResponse " " >> w xs
                 in queueResponse " [" >> w attrs'
     queueResponse resp
     queueResponse "\r\n"
     finishResponse

sendResponseOk :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseOk t = sendResponse t ResponseStateOk

sendResponseBad :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseBad t = sendResponse t ResponseStateBad

lookaheadByte :: ImapServer Word8
lookaheadByte = ImapServer worker
  where worker state =
          do inbuf <- readIORef $ iss_inbuf state
             if iss_inbuf_idx state >= BS.length inbuf
               then do nextChunk <- BS.hGetSome (iss_handle state) 4096
                       trace ("grabbed chunk " ++ (byteStringToString nextChunk)) $
                         if BS.length nextChunk == 0
                         then return $ Left ImapStopFinished
                         else do modifyIORef (iss_inbuf state) (flip BS.append nextChunk)
                                 worker state
               else return $ Right (state, BS.index inbuf (iss_inbuf_idx state))

readByte :: ImapServer Word8
readByte = lookaheadByte >>= \res ->
  ImapServer $ \state -> return $ Right (state {iss_inbuf_idx = (iss_inbuf_idx state) + 1},
                                         res)
                                     
readCountedBytes :: Int -> ImapServer [Word8]
readCountedBytes 0 = return []
readCountedBytes x =
  do c <- readByte
     c' <- readCountedBytes (x - 1)
     return $ c:c'

readCountedChars :: Int -> ImapServer String
readCountedChars n = liftM (map (chr.fromInteger.toInteger)) $ readCountedBytes n

readChar :: ImapServer Char
readChar = liftM (chr.fromInteger.toInteger) readByte

lookaheadChar :: ImapServer Char
lookaheadChar = liftM (chr.fromInteger.toInteger) lookaheadByte

parseBacktrack :: ImapServer a
parseBacktrack = ImapServer $ \_ -> return $ Left ImapStopBacktrack

alternates :: [ImapServer a] -> ImapServer a
alternates options = ImapServer $ worker options
  where worker [] _ = return $ Left $ ImapStopBacktrack
        worker (opt1:opts) state =
          do res1 <- run_is opt1 state
             case res1 of
               Left ImapStopBacktrack -> worker opts state
               Left err -> return $ Left err
               Right res2 -> return $ Right res2
optional :: ImapServer a -> ImapServer (Maybe a)        
optional what = alternates [liftM Just what, return Nothing]

runWithErrs :: ImapServer a -> ImapServer (Either String a)
runWithErrs underlying = ImapServer $ \state ->
  do res <- run_is underlying state
     return $ Right $ case res of
       Left (ImapStopFailed errMsg) -> (state, Left errMsg)
       Left (ImapStopFinished) -> (state, Left "reached the end")
       Left (ImapStopBacktrack) -> (state, Left "ran out of backtrack")
       Right (state', x) -> (state', Right x)
       
imapParserFlush :: ImapServer ()
imapParserFlush = ImapServer $ \state -> do modifyIORef (iss_inbuf state) (BS.drop (iss_inbuf_idx state))
                                            return $ Right (state { iss_inbuf_idx = 0 }, ())

skipToEOL :: ImapServer String
skipToEOL = do c <- readChar
               if c == '\r'
                 then state1
                 else liftM ((:) c) skipToEOL
            where state1 = do c <- readChar
                              if c == '\n'
                                then return ""
                                else if c == '\r'
                                     then state1
                                     else liftM ((:) c) skipToEOL
                                          
readCommand :: ImapServer (ResponseTag, ImapCommand)
readCommand =
  do t <- liftM ResponseTagged readTag
     requireChar ' '
     cmd <- alternates [alternates $ map (\(name, x) -> do requireString name
                                                           r <- x
                                                           requireEOL
                                                           return r) commandParsers,
                        do l <- skipToEOL
                           return $ ImapCommandBad l
                       ]
     imapParserFlush
     return (t, cmd)
  where readTag = alternates [do c <- readChar
                                 if c == ' '
                                   then parseBacktrack
                                   else liftM ((:) c) readTag,
                              return ""]
        requireChar c = do c' <- readChar
                           if c == c'
                             then return ()
                             else parseBacktrack
        requireString = sequence_ . (map requireChar)
        requireEOL = requireString "\r\n"
        parseAstring = alternates [parseQuoted, parseLiteral, parseMany1 parseAstringChar]
        parseQuoted = requireChar '"' >> worker
          where worker = do c <- readChar
                            case c of
                              '"' -> return ""
                              '\\' -> do c' <- readChar
                                         liftM ((:) c') worker
                              _ -> liftM ((:) c) worker
        parseNumber =
          do c <- lookaheadChar
             if not $ c `elem` "1234567890"
               then parseBacktrack
               else worker 0
               where worker acc =
                       do c <- lookaheadChar
                          if not (c `elem` "1234567890")
                            then return acc
                            else do _ <- readChar
                                    worker $ (acc * 10) + (digitToInt c)
        parseLiteral = do requireChar '{'
                          cnt <- parseNumber
                          requireString "}\r\n"
                          readCountedChars cnt
        parseMany1 what = do r <- what
                             alternates [do res <- parseMany1 what
                                            return $ r:res,
                                         return [r]]
        parseMany1Sep sep elm = do r <- elm
                                   alternates [ do _ <- sep
                                                   res <- parseMany1Sep sep elm
                                                   return $ r:res,
                                                return [r] ]
        parseAstringChar = do c <- readChar
                              if (c `elem` "(){ %*\"\\") || (ord c < 32)
                                then parseBacktrack
                                else return c
        commandParsers = [("NOOP", return ImapNoop),
                          ("CAPABILITY", return ImapCapability),
                          ("LOGIN", do requireChar ' '
                                       username <- parseAstring
                                       requireChar ' '
                                       password <- parseAstring
                                       return $ ImapLogin username password),
                          ("LIST", do requireChar ' '
                                      referenceName <- parseMailbox
                                      requireChar ' '
                                      listMailbox <- parseListMailbox
                                      return $ ImapList referenceName listMailbox),
                          ("SELECT", do requireChar ' '
                                        liftM ImapSelect parseMailbox),
                          ("FETCH", do requireChar ' '
                                       seqs <- parseSequenceSet
                                       requireChar ' '
                                       attrs <- parseFetchAttributes
                                       return $ ImapFetch seqs attrs),
                          ("UID FETCH ", do uids <- parseUidSet
                                            requireChar ' '
                                            attrs <- parseFetchAttributes
                                            return $ ImapFetchUid uids attrs)]

        parseFetchAttributes = alternates [do requireString "ALL"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size,
                                                      FetchAttrEnvelope],
                                           do requireString "FAST"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size],
                                           do requireString "FULL"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size,
                                                      FetchAttrEnvelope,
                                                      FetchAttrBody False Nothing Nothing],
                                           do requireChar '('
                                              r <- parseMany1Sep (requireChar ' ') parseFetchAttr
                                              requireChar ')'
                                              return r,
                                           liftM (\x -> [x]) parseFetchAttr]
        parseSequenceSet = liftM concat $ parseMany1Sep (requireChar ',') $ do n <- parseSeqNumber
                                                                               alternates [ do requireChar ':'
                                                                                               r <- parseSeqNumber
                                                                                               return [n..r],
                                                                                            return [n]]
        parseUidSet = liftM concat $ parseMany1Sep (requireChar ',') $ do n <- parseUid
                                                                          m <- optional $ do requireChar ':'
                                                                                             parseUid
                                                                          return $ case m of
                                                                            Nothing -> [n]
                                                                            Just m' -> [n..m']
        parseSeqNumber = alternates [liftM MsgSequenceNumber parseNumber,
                                     (requireChar '*' >> getMaxSeqNumber)]
        getMaxSeqNumber = ImapServer $ \state -> return $ Right (state, MsgSequenceNumber $ (length $ iss_uids state) + 1)
        parseUid = liftM MsgUid parseNumber
        parseSection = do requireChar '['
                          r <- optional parseSectionSpec
                          requireChar ']'
                          return r
        parseSectionSpec = alternates [parseSectionMsgText,
                                       do part <- parseSectionPart
                                          rest <- optional $ (requireChar '.' >> parseSectionText)
                                          return $ SectionSubPart part rest]
        parseSectionMsgText = alternates [(requireString "TEXT" >> return SectionMsgText),
                                          do requireString "HEADER.FIELDS"
                                             invert <- alternates [requireString "NOT" >> return True,
                                                                   return False]
                                             requireChar ' '
                                             headers <- parseHeaderList
                                             return $ SectionMsgHeaderFields invert headers,
                                          (requireString "HEADER" >> return SectionMsgHeader)]
        parseSectionPart = liftM MimeSectionPath $ parseMany1Sep (requireChar '.') parseNumber
        parseSectionText = alternates [parseSectionMsgText, (requireString "MIME" >> return SectionMsgMime)]
        parseHeaderList = do requireChar '('
                             r <- parseMany1Sep (requireChar ' ') parseHeaderFldName
                             requireChar ')'
                             return r
        parseHeaderFldName = parseAstring
        parseFetchAttr = alternates [(requireString "ENVELOPE" >> return FetchAttrEnvelope),
                                     (requireString "FLAGS" >> return FetchAttrFlags),
                                     (requireString "INTERNALDATE" >> return FetchAttrInternalDate),
                                     (requireString "RFC822.HEADER" >> return FetchAttrRfc822Header),
                                     (requireString "RFC822.SIZE" >> return FetchAttrRfc822Size),
                                     (requireString "RFC822.TEXT" >> return FetchAttrRfc822Text),
                                     (requireString "RFC822" >> return FetchAttrRfc822),
                                     (requireString "UID" >> return FetchAttrUid),
                                     (requireString "BODYSTRUCTURE" >> return FetchAttrBodyStructure),
                                     (requireString "BODY" >>
                                      do peek <- alternates [(requireString ".PEEK" >> return True),
                                                             return False]
                                         section <- liftM join $ optional parseSection
                                         byteRange <- optional $ do requireChar '<'
                                                                    l <- parseNumber
                                                                    requireChar '.'
                                                                    r <- parseNumber
                                                                    requireChar '>'
                                                                    return $ ByteRange l r
                                         return $ FetchAttrBody peek section byteRange)]
        parseMailbox = do s <- parseAstring
                          return $ if "inbox" == map toLower s 
                                   then "INBOX"
                                   else s
        parseListMailbox = alternates [parseLiteral,
                                       parseQuoted,
                                       parseMany1 parseListChar]
        parseListChar = do c <- readChar
                           if (c `elem` "(){ \\\"") || (ord c < 32)
                             then parseBacktrack
                             else return c


checkValidMbox :: String -> ImapServer Bool
checkValidMbox _ = return True -- UNIMPLEMENTED

findAttribute :: String -> ImapServer DS.SQLData
findAttribute name =
  ImapServer $ \state ->
  return $
  Right $
  (state, maybe (error $ "unknown message attribute " ++ name) id $
          lookup name $ iss_attributes state)

dbQuery :: String -> [DS.SQLData] -> ImapServer [[DS.SQLData]]
dbQuery what binders = ImapServer $ \state ->
  let db = iss_database state
      fetchAllRows stmt =
        do r <- DS.step stmt
           case r of
             DS.Done -> return []
             DS.Row ->
               do v <- DS.columns stmt
                  liftM ((:) v) $ fetchAllRows stmt
  in liftM (Right . ((,) state)) $
     do prepped <- DS.prepare db $ DT.pack what
        (DS.bind prepped binders >> fetchAllRows prepped) `finally` (DS.finalize prepped)

unpackMsgUid :: DS.SQLData -> MsgUid
unpackMsgUid (DS.SQLInteger i) = MsgUid $ fromInteger $ toInteger i
unpackMsgUid other = error $ "Bad msg UID " ++ (show other)

processCommand :: Either String (ResponseTag, ImapCommand) -> ImapServer ()
processCommand (Left err) =
  trace ("Failed: " ++ err) $ sendResponseBad ResponseUntagged [] $ "Failed: " ++ err
processCommand (Right (tag, cmd)) =
  trace ("Run command " ++ (show cmd)) $
  case cmd of
    ImapCapability -> do sendResponse ResponseUntagged ResponseStateNone [] "CAPABILITY IMAP4rev1"
                         sendResponseOk tag [] "Done capability"
    ImapNoop -> sendResponseOk tag [] "Done noop"
    ImapLogin username password -> trace ("login as " ++ username ++ ", password " ++ password) $
                                   sendResponseOk tag [] "Logged in"
    ImapList reference mailbox ->
      if reference /= "" || (not $ mailbox `elem` ["", "INBOX"])
      then sendResponseBad tag [] "LIST bad mailbox or reference"
      else do sendResponse ResponseUntagged ResponseStateNone [] "LIST () \"/\" \"INBOX\""
              sendResponseOk tag [] "LIST completed"
    ImapSelect mailbox ->
      do isValidMbox <- checkValidMbox mailbox
         if not isValidMbox
           then sendResponseBad tag [] "SELECT non-existent mailbox"
           else do mboxAttr <- findAttribute "harbinger.mailbox"
                   recentAttr <- findAttribute "harbinger.recent"
                   seenAttr <- findAttribute "harbinger.seen"
                   let sqlMbox = DS.SQLText $ DT.pack mailbox
                       countByFlag flagAttr =
                         dbQuery "SELECT COUNT(*) FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1" [mboxAttr, sqlMbox, flagAttr]
                   uids <- liftM (map $ unpackMsgUid . head) $ dbQuery "SELECT DISTINCT MessageId FROM MessageAttrs WHERE AttributeId = ? AND Value = ?" [mboxAttr, sqlMbox]
                   sendResponse ResponseUntagged ResponseStateNone [] $ (show $ length uids) ++ " EXISTS"
                   recent <- countByFlag recentAttr
                   sendResponse ResponseUntagged ResponseStateNone [] $ (show $ length recent) ++ " RECENT"
                   sendResponse ResponseUntagged ResponseStateNone [] "FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft \\Recent)"
                   seen <- liftM (sort . map (unpackMsgUid . head)) $ dbQuery "SELECT attr1.MessageId FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1 ORDER BY attr1.MessageId" [mboxAttr, sqlMbox, seenAttr]
                   case seen of
                     [] -> return ()
                     (x:_) -> case elemIndex x uids of
                       Nothing -> return ()
                       Just idx -> sendResponse ResponseUntagged ResponseStateNone [] $ "OK [UNSEEN " ++ (show idx) ++ "]"
                   sendResponse ResponseUntagged ResponseStateNone [] "OK [UIDVALIDITY 12345]"
                   nextUid <- dbQuery "SELECT Val FROM NextMessageId" []
                   case nextUid of
                     [[DS.SQLInteger i]] ->
                       sendResponse ResponseUntagged ResponseStateNone [] $ "OK [UIDNEXT " ++ (show i) ++ "]"
                     _ -> error $ "NextMessageId query produced unexpected result " ++ (show nextUid)
                   sendResponse ResponseUntagged ResponseStateNone [] "OK [PERMANENTFLAGS ()]"
                   sendResponseOk tag [ResponseAttribute "READ-WRITE"] "SELECT completed"
                   ImapServer $ \state -> return $ Right (state {iss_uids = uids}, ())
    ImapFetch sequenceNumbers attributes ->
      do sequence_ $ map (fetchMessage attributes) sequenceNumbers
         sendResponseOk tag [] "FETCH completed"
    ImapFetchUid uids attributes ->
      do sequence_ $ map (fetchMessageUid attributes) uids
         sendResponseOk tag [] "UID FETCH complete"
    ImapCommandBad l -> sendResponseBad tag [] $ "Bad command " ++ l

data Message = Message

-- For now, only one message.
loadMessage :: MsgSequenceNumber -> ImapServer Message
loadMessage (MsgSequenceNumber seqNr) = ImapServer $ \state ->
  let uids = iss_uids state
  in if seqNr <= 0 || seqNr > length uids
     then return $ Left $ ImapStopFailed $ "invalid message sequence number " ++ (show seqNr) ++ "; max " ++ (show $ length uids)
     else run_is (loadMessageByUid $ uids !! seqNr) state

loadMessageByUid :: MsgUid -> ImapServer Message
loadMessageByUid _ = return Message

extractMessageHeader :: String -> Message -> Maybe String
extractMessageHeader "FROM" _ = Just "From: fromaddr"
extractMessageHeader "TO" _ = Just "To: toaddr"
extractMessageHeader "SUBJECT" _ = Just "Subject: the subject line"
extractMessageHeader "DATE" _ = Just "Date: 1983-Apr-24"
extractMessageHeader _ _ = Nothing

extractMessageHeaders :: Bool -> [String] -> Message -> String
extractMessageHeaders False headers msg =
  (intercalate "\r\n" $ dropNothing $ map (flip extractMessageHeader msg) headers) ++ "\r\n"
  where dropNothing [] = []
        dropNothing (Nothing:x) = dropNothing x
        dropNothing ((Just y):x) = y:(dropNothing x)
        
renderLiteralString :: String -> String
renderLiteralString x =
  "{" ++ (show $ length x) ++ "}\r\n" ++ x
  
grabMessageAttribute :: FetchAttribute -> Message -> ImapServer [(String, String)]
grabMessageAttribute attr msg =
  case attr of
    FetchAttrUid -> return [("UID", "7")]
    FetchAttrFlags -> return [("FLAGS", "(\\Seen)")]
    FetchAttrInternalDate -> return [("INTERNALDATE", "\"10-Apr-2004\"")] -- date must be in double quotes to keep mutt happy
    FetchAttrRfc822Size -> return [("RFC822.SIZE", "99")]
    FetchAttrBody _peek (Just (SectionMsgHeaderFields invert headers)) Nothing ->
      return [("BODY", "BODY[HEADER.FIELDS (" ++ (intercalate " " headers) ++ ")] " ++ (renderLiteralString $ extractMessageHeaders invert headers msg) ++ "\r\n")]
    FetchAttrBody _peek Nothing Nothing ->
      return [("UID", "7"),
              ("BODY[]", renderLiteralString "From: fromaddr\r\nTo: toaddr\r\nSubject: the subject line\r\nDate: 1983-Apr-24\r\n\r\nThis is the body\r\n")]
      
fetchMessage :: [FetchAttribute] -> MsgSequenceNumber -> ImapServer ()
fetchMessage attrs seqNr@(MsgSequenceNumber i) =
  do msg <- loadMessage seqNr
     results <- mapM (flip grabMessageAttribute msg) attrs
     let res = concatMap (concatMap $ \(a, b) -> [a,b]) results
     sendResponse ResponseUntagged ResponseStateNone [] $ (show i) ++ " FETCH (" ++ (intercalate " " res) ++ ")"

fetchMessageUid :: [FetchAttribute] -> MsgUid -> ImapServer ()
fetchMessageUid attrs uid@(MsgUid i) =
  do msg <- loadMessageByUid uid
     results <- mapM (flip grabMessageAttribute msg) attrs
     let res = concatMap (concatMap $ \(a, b) -> [a,b]) results
     sendResponse ResponseUntagged ResponseStateNone [] $ (show i) ++ " FETCH (" ++ (intercalate " " res) ++ ")"
  
untilError :: ImapServer a -> ImapServer a
untilError server = ImapServer $ \state ->
  do r <- run_is server state
     case r of
       Left st -> return $ Left st
       Right (state', _) -> run_is (untilError server) state'
       
processClient :: Handle -> IO ()
processClient clientHandle =
  runImapServer clientHandle $ do sendResponseOk ResponseUntagged [] "Hello"
                                  untilError (runWithErrs readCommand >>= processCommand)
     
main :: IO ()
main =
  withSocketsDo $
  do listenSock <- listenOn $ PortNumber 5000
     forever $ do (clientHandle, clientHost, clientPort) <- accept listenSock
                  print $ "Accepted from " ++ clientHost ++ ", " ++ (show clientPort)
                  ignore $ forkIO $  finally (processClient clientHandle) (hClose clientHandle)
