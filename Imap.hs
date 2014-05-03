import Network
import System.IO
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Char
import Data.Int
import Data.IORef
import Data.List
import Debug.Trace
import qualified Database.SQLite3 as DS
import qualified Data.Text as DT
import qualified Data.ByteString as BS

import Deliver
import Email
import Util

import ImapParser
import ImapServer

ignore :: Monad m => m a -> m ()
ignore = flip (>>) $ return ()

sortUids :: [MsgUid] -> [MsgUid]
sortUids = map DS.SQLInteger . sort . map (\x -> case x of
                                              DS.SQLInteger i -> i
                                              _ -> error $ "bad uid to sort (" ++ (show x) ++ ")")
  
loadAttributes :: ImapServer ()
loadAttributes =
  do attribs <- dbQuery "SELECT AttributeId, Description FROM Attributes" []
     let attribs' = flip map attribs $ \r ->
           case r of
             [attribId, DS.SQLText description] -> (DT.unpack description, attribId)
             _ -> error $ "Bad attribute definition " ++ (show r)
     ImapServer $ \state -> return $ Right (state {iss_attributes = attribs'}, ())

loadMailboxList :: ImapServer ()
loadMailboxList =
  do mailboxes <- dbQuery "SELECT Name FROM MailBoxes" []
     let mboxes = flip map mailboxes $ \r ->
           case r of
             [DS.SQLText name] -> name
             _ -> error $ "Bad mailbox definition " ++ (show r)
     ImapServer $ \state -> return $ Right (state {iss_mailboxes = mboxes}, ())
     
runImapServer :: Handle -> ImapServer () -> IO ()
runImapServer hndle is =
  do inbuf <- newIORef BS.empty
     database <- DS.open $ DT.pack "harbinger.db"
     ignore $ run_is (loadAttributes >> loadMailboxList >> is) $
       ImapServerState { iss_handle = Left hndle, 
                         iss_outgoing_response = [],
                         iss_inbuf = inbuf, 
                         iss_inbuf_idx = 0,
                         iss_messages = [], 
                         iss_database = database, 
                         iss_selected_mbox = Nothing,
                         iss_attributes = error "failed to load attributes DB?",
                         iss_mailboxes = error "failed to load mailbox list"}

data ResponseState = ResponseStateOk
                   | ResponseStateNo
                   | ResponseStateBad
                   | ResponseStateNone
                     deriving Show
data ResponseAttribute = ResponseAttribute String
                         deriving Show
attrToString :: ResponseAttribute -> String
attrToString (ResponseAttribute x) = x

sendResponse_ :: ResponseTag -> ResponseState -> [ResponseAttribute] -> Either String BS.ByteString -> ImapServer ()
sendResponse_ tag state attrs resp =
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
     case resp of
       Left resp' -> queueResponse resp'
       Right resp' -> queueResponseBs resp'
     queueResponse "\r\n"
     finishResponse

sendResponse :: ResponseTag -> ResponseState -> [ResponseAttribute] -> String -> ImapServer ()
sendResponse tag state attrs resp =
  (sendResponse_ tag state attrs . Left) resp

sendResponseBs :: ResponseTag -> ResponseState -> [ResponseAttribute] -> BS.ByteString -> ImapServer ()
sendResponseBs tag state attrs resp =
  (sendResponse_ tag state attrs . Right) resp

sendResponseOk :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseOk t = sendResponse t ResponseStateOk

sendResponseBad :: ResponseTag -> [ResponseAttribute] -> String -> ImapServer ()
sendResponseBad t = sendResponse t ResponseStateBad

runWithErrs :: ImapServer a -> ImapServer (Either String a)
runWithErrs underlying = ImapServer $ \state ->
  do res <- run_is underlying state
     return $ Right $ case res of
       Left (ImapStopFailed errMsg) -> (state, Left errMsg)
       Left (ImapStopFinished) -> (state, Left "reached the end")
       Left (ImapStopBacktrack) -> (state, Left "ran out of backtrack")
       Right (state', x) -> (state', Right x)
       
checkValidMbox :: String -> ImapServer Bool
checkValidMbox name =
  let name' = map toLower name in
  ImapServer $ \state ->
  return $ Right $ ((,) state) $
  case find ((==) name' . (map toLower . DT.unpack)) $ iss_mailboxes state of
    Nothing -> False
    Just _ -> True

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

uidToSequenceNumber :: MsgUid -> ImapServer MsgSequenceNumber
uidToSequenceNumber uid = ImapServer $ \state ->
  case findIndex (\x -> case x of
                     Left u -> u == uid
                     Right r -> msg_uid r == uid) $ iss_messages state of
    Nothing -> error $ "Request for unknown uid " ++ (show uid)
    Just l -> return $ Right (state, MsgSequenceNumber (l + 1))
storeUids :: ResponseTag -> Maybe Bool -> Bool -> [MessageFlag] -> [MsgUid] -> ImapServer ()
storeUids tag mode silent flags uids =
  let doOne uid =
        do msg <- loadMessageByUid uid
           case mode of
             Nothing -> sequence_ $ flip map allMessageFlags $ \flag ->
               (if flag `elem` flags
                then setFlag
                else clearFlag) flag msg
             Just True -> sequence_ $ map (flip setFlag msg) flags
             Just False -> sequence_ $ map (flip clearFlag msg) flags
           if not silent
             then do newFlags <- grabMessageAttribute FetchAttrFlags msg
                     seqnr <- uidToSequenceNumber uid
                     sendFetchResponse seqnr newFlags
             else return ()
  in do sequence_ $ map doOne uids
        sendResponseOk tag [] "UID STORE complete"
           
expunge :: Bool -> ImapServer Bool
expunge sendUntagged =
  do msgs <- ImapServer $ \state -> return $ Right (state, iss_messages state)
     mbox' <- ImapServer $ \state -> return $ Right (state, iss_selected_mbox state)
     case mbox' of
       Nothing -> return False
       Just mbox ->
         do toDelete <- foldM (\acc msg ->
                                case msg of
                                  Left _ -> return acc
                                  Right msg' ->
                                    do flg <- liftServer $ readIORef $ msg_deleted msg'
                                       trace ((show $ msg_uid msg') ++ " deleted = " ++ (show flg)) $
                                         if flg
                                         then do seqnr <- uidToSequenceNumber $ msg_uid msg'
                                                 return $ (seqnr, msg_uid msg'):acc
                                         else return acc)
                        []
                        msgs
            mboxAttr <- trace ("Going to delete " ++ (show toDelete)) $ findAttribute "harbinger.mailbox"
            let worker acc ((MsgSequenceNumber seqNr), uid) =
                  if not acc
                  then return False
                  else do _ <- dbQuery
                               "DELETE FROM MessageAttrs WHERE MessageId = ? AND AttributeId = ? AND Value = ?"
                               [uid, mboxAttr, DS.SQLText mbox]
                          ImapServer $ \state ->
                            return $ Right (state { iss_messages = flip filter (iss_messages state) $
                                                                   \x ->
                                                                   case x of
                                                                     Left y -> y /= uid
                                                                     Right y -> msg_uid y /= uid},
                                            ())
                          if sendUntagged
                             then sendResponse ResponseUntagged ResponseStateNone [] $
                                  (show seqNr) ++ " EXPUNGE"
                            else return ()
                          return True
            foldM worker True toDelete

imapPatternCheck :: String -> String -> Bool
imapPatternCheck "" "" = True
imapPatternCheck "" _ = False
imapPatternCheck (p:ps) string =
  if p `elem` "%*"
  then or $ map (imapPatternCheck ps) $ tails string
  else case string of
    (s:ss) | p == s -> imapPatternCheck ps ss
    _ -> False
    
imapListCommand :: ResponseTag -> String -> String -> ImapServer Bool
imapListCommand _ "" pattern =
  do res <- ImapServer $ \state -> return $ Right (state,
                                                   filter (imapPatternCheck pattern . DT.unpack) $ iss_mailboxes state)
     flip mapM_ res $ \mbox ->
       sendResponse ResponseUntagged ResponseStateNone [] $ "LIST (\\NoInferiors) \"\" " ++ (renderQuotedString $ DT.unpack mbox)
     return True
imapListCommand tag _ _ =
  do sendResponseBad tag [] "LIST bad mailbox or reference"
     return False

dbQueryInt :: String -> [DS.SQLData] -> ImapServer Int64
dbQueryInt q params =
  do res <- dbQuery q params
     return $ case res of
       [[DS.SQLInteger i]] -> i
       _ -> error $ "Expected query " ++ q ++ " to produce an int, got " ++ (show res)
       
imapStatus :: ResponseTag -> String -> [StatusItem] -> ImapServer ()
imapStatus tag mboxName items =
  let sqlMbox = DS.SQLText $ DT.pack mboxName in
  do valid <- checkValidMbox mboxName
     mboxAttr <- findAttribute "harbinger.mailbox"
     recentAttr <- findAttribute "harbinger.recent"
     seenAttr <- findAttribute "harbinger.seen"
     let fromDb what q params = do dbres <- dbQueryInt q params
                                   return $ (statusItemName what) ++ " " ++ (show dbres)
         worker StatusItemMessages =
           fromDb
           StatusItemMessages
           "SELECT COUNT(DISTINCT MessageId) FROM MessageAttrs WHERE AttributeId = ? AND Value = ?"
           [mboxAttr, sqlMbox]
         worker StatusItemRecent =
           fromDb
           StatusItemRecent
           "SELECT COUNT(DISTINCT attr1.MessageId) FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1"
           [mboxAttr, sqlMbox, recentAttr]
         worker StatusItemUidNext =
           fromDb
           StatusItemUidNext
           "SELECT Val FROM NextMessageId"
           []
         worker StatusItemUidValidity =
           return "UIDVALIDITY 12345"
         worker StatusItemUnseen =
           do seen <- dbQueryInt
                      "SELECT COUNT(DISTINCT attr1.MessageId) FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1"
                      [mboxAttr, sqlMbox, seenAttr]
              total <- dbQueryInt
                       "SELECT COUNT(DISTINCT MessageId) FROM MessageAttrs WHERE AttributeId = ? AND Value = ?"
                       [mboxAttr, sqlMbox]
              return $ "UNSEEN " ++ (show $ total - seen)
     if not valid
       then sendResponseBad tag [] "STATUS bad mbox"
       else do items' <- mapM worker items
               sendResponse ResponseUntagged ResponseStateNone [] $ "STATUS " ++ mboxName ++ " (" ++ (intercalate " " items') ++ ")"
               sendResponseOk tag [] "STATUS complete"
       
processCommand :: Either String (ResponseTag, ImapCommand) -> ImapServer ()
processCommand (Left err) =
  trace ("Failed: " ++ err) $ sendResponseBad ResponseUntagged [] $ "Failed: " ++ err
processCommand (Right (tag, cmd)) =
  trace ("Run command " ++ (show cmd)) $
  case cmd of
    ImapCapability -> do cap <- ImapServer $ \state -> return $ Right (state, case iss_handle state of
                                                                          Left _nonSsl -> " STARTTLS LOGINDISABLED"
                                                                          Right _ssl -> "")
                         sendResponse ResponseUntagged ResponseStateNone [] $ "CAPABILITY IMAP4rev1" ++ cap
                         sendResponseOk tag [] "Done capability"
    ImapNoop -> sendResponseOk tag [] "Done noop"
    ImapLogin username password ->
      do allowed <- ImapServer $ \state -> return $ Right (state, case iss_handle state of
                                                              Left _nonSsl -> False
                                                              Right _ssl -> True)
         if allowed
           then trace ("login as " ++ username ++ ", password " ++ password) $
                sendResponseOk tag [] "Logged in"
           else sendResponseBad tag [] "no login without SSL"
    ImapList reference mailbox ->
      do r <- imapListCommand tag reference mailbox
         if r
           then sendResponseOk tag [] "LIST completed"
           else return ()
    ImapSelect mailbox ->
      do isValidMbox <- checkValidMbox mailbox
         if not isValidMbox
           then sendResponseBad tag [] "SELECT non-existent mailbox"
           else do mboxAttr <- findAttribute "harbinger.mailbox"
                   recentAttr <- findAttribute "harbinger.recent"
                   seenAttr <- findAttribute "harbinger.seen"
                   let sqlMbox = DS.SQLText $ DT.pack mailbox
                       countByFlag flagAttr =
                         dbQuery "SELECT COUNT(DISTINCT attr1.MessageId) FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1" [mboxAttr, sqlMbox, flagAttr]
                   uids <- liftM (map head) $ dbQuery "SELECT DISTINCT MessageId FROM MessageAttrs WHERE AttributeId = ? AND Value = ?" [mboxAttr, sqlMbox]
                   sendResponse ResponseUntagged ResponseStateNone [] $ (show $ length uids) ++ " EXISTS"
                   recent <- countByFlag recentAttr
                   sendResponse ResponseUntagged ResponseStateNone [] $ (show $ length recent) ++ " RECENT"
                   sendResponse ResponseUntagged ResponseStateNone [] "FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft \\Recent)"
                   seen <- liftM (sortUids . map head) $ dbQuery "SELECT attr1.MessageId FROM MessageAttrs AS attr1 JOIN MessageAttrs AS attr2 ON attr1.AttributeId = ? AND attr1.Value = ? AND attr1.MessageId = attr2.MessageId WHERE attr2.AttributeId = ? AND attr2.Value = 1 ORDER BY attr1.MessageId" [mboxAttr, sqlMbox, seenAttr]
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
                   ImapServer $ \state -> return $ Right (state {iss_messages = map Left uids,
                                                                 iss_selected_mbox = Just $ DT.pack mailbox}, ())
    ImapFetch sequenceNumbers attributes ->
      do sequence_ $ map (fetchMessage attributes) sequenceNumbers
         sendResponseOk tag [] "FETCH completed"
    ImapFetchUid uids attributes ->
      do sequence_ $ map (fetchMessageUid attributes) uids
         sendResponseOk tag [] "UID FETCH complete"
    ImapStoreUid uids mode silent flags ->
      storeUids tag mode silent flags uids
    ImapExpunge -> do r <- expunge True
                      if r
                        then sendResponseOk tag [] "EXPUNGE complete"
                        else sendResponseBad tag [] "EXPUNGE failed"
    ImapStatus mbox items -> imapStatus tag mbox items
    ImapClose -> do r <- expunge False
                    if r
                      then do ImapServer $ \state -> return $ Right (state {iss_selected_mbox = Nothing,
                                                                            iss_messages = []},
                                                                     ())
                              sendResponseOk tag [] "CLOSE complete"
                      else sendResponseBad tag [] "CLOSE failed"
    ImapLogout -> do sendResponse ResponseUntagged ResponseStateNone [] "BYE Logging out"
                     sendResponseOk tag [] "LOGOUT"
                     ImapServer $ \_ -> return $ Left ImapStopFinished
    ImapAppend mailbox flags datetime body ->
      do valid <- checkValidMbox mailbox
         if not valid
           then sendResponseBad tag [] "APPEND bad mailbox"
           else
           case runErrorable $ parseEmail body of
             Left _ -> sendResponseBad tag [] "APPEND bad message"
             Right parsed ->
               do success <- ImapServer $ \state ->
                    do s <- fileEmail (Just mailbox) flags (iss_database state) (iss_attributes state) parsed
                       return $ Right (state, s)
                  if success
                    then sendResponseOk tag [] "APPEND complete"
                    else sendResponseBad tag [] "APPEND failed"
    ImapCommandBad l -> sendResponseBad tag [] $ "Bad command " ++ l

loadMessage :: MsgSequenceNumber -> ImapServer Message
loadMessage (MsgSequenceNumber seqNr) = ImapServer $ \state ->
  let messages = iss_messages state
  in if seqNr <= 0 || seqNr > length messages
     then return $ Left $ ImapStopFailed $ "invalid message sequence number " ++ (show seqNr) ++ "; max " ++ (show $ 1 + length messages)
     else case messages !! (seqNr - 1) of
       Left l -> run_is (loadMessageByUid l) state
       Right r -> return $ Right (state, r)

liftServer :: IO a -> ImapServer a
liftServer what = ImapServer $ \state ->
  liftM (Right . (,) state) what

loadMessageByUid :: MsgUid -> ImapServer Message
loadMessageByUid uid =
  do cached <- ImapServer $ \state ->
       return $ Right (state,
                       flip find (iss_messages state) $ \entry ->
                       case entry of
                         Left _ -> False
                         Right x | uid == msg_uid x -> True
                         _ -> False)
     case cached of
       Just (Right r) -> return r
       _ ->
         do pathQ <- dbQuery "SELECT Location FROM Messages WHERE MessageId = ?" [uid]
            let path = case pathQ of
                  [[DS.SQLText pth]] -> DT.unpack pth
                  _ -> error $ "Unexpected message path " ++ (show pathQ) ++ " for UID " ++ (show uid)
            content <- liftServer $ BS.readFile path
            case runErrorable $ parseEmail content of
              Left errs -> error $ "Cannot parse " ++ path ++ " which is already in the database? (" ++ (show errs) ++ ")"
              Right eml ->
                do deleted <- liftServer $ newIORef False
                   let r = Message { msg_email = eml,
                                     msg_deleted = deleted,
                                     msg_uid = uid }
                   ImapServer $ \state ->
                     return $ Right (state { iss_messages = flip map (iss_messages state) $
                                                            \x -> case x of
                                                              Left u | u == uid -> Right r
                                                              Right rr | uid == msg_uid rr -> Right r
                                                              _ -> x},
                                     r)

{- Grab a message header, including both the header name and the value components. -}
extractMessageHeaderFull :: String -> Email -> Maybe String
extractMessageHeaderFull headerName eml =
  let hn = map toLower headerName in
  fmap (\(Header h v) -> h ++ ": " ++ v) $ find (\(Header h _) -> (map toLower h) == hn) $ eml_headers eml
{- Just the value of the header -}
extractMessageHeader :: String -> Email -> Maybe String
extractMessageHeader headerName eml =
  let hn = map toLower headerName in
  fmap (\(Header _ v) -> v) $ find (\(Header h _) -> (map toLower h) == hn) $ eml_headers eml

extractFullMessage :: Email -> BS.ByteString
extractFullMessage eml =
  BS.intercalate (stringToByteString "\r\n") $
  foldr (\(Header h v) rest -> (stringToByteString $ h ++ ": " ++ v):rest) [BS.empty, eml_body eml] $
  eml_headers eml

extractMessageHeaders :: Bool -> [String] -> Email -> String
extractMessageHeaders False headers msg =
  (intercalate "\r\n" $ dropNothing $ map (flip extractMessageHeaderFull msg) headers) ++ "\r\n"
  where dropNothing [] = []
        dropNothing (Nothing:x) = dropNothing x
        dropNothing ((Just y):x) = y:(dropNothing x)
        
renderLiteralByteString :: BS.ByteString -> BS.ByteString
renderLiteralByteString x =
  BS.concat [ stringToByteString "{",
              stringToByteString $ show $ BS.length x,
              stringToByteString "}\r\n",
              x]
renderLiteralString :: String -> String
renderLiteralString x =
  concat [ "{",
           show $ length x,
           "}\r\n",
           x]
renderQuotedString :: String -> String  
renderQuotedString x = "\"" ++ x ++ "\""

uidToByteString :: MsgUid -> BS.ByteString
uidToByteString (DS.SQLInteger i) = stringToByteString $ show i
uidToByteString a = error $ "non-integer uid " ++ (show a)

attributeId :: String -> ImapServer DS.SQLData
attributeId name = ImapServer $ \state ->
  return $ Right (state,
                  maybe
                    (error $ "Bad message attribute " ++ name)
                    id
                    (lookup name $ iss_attributes state))
  
fetchMessageFlag :: MessageFlag -> Message -> ImapServer Bool
fetchMessageFlag MessageFlagSeen = fetchSimpleFlag "harbinger.seen"
fetchMessageFlag MessageFlagRecent = fetchSimpleFlag "harbinger.recent"
fetchMessageFlag MessageFlagDeleted = liftServer . readIORef . msg_deleted

fetchSimpleFlag :: String -> Message -> ImapServer Bool
fetchSimpleFlag flagName eml =
  let uid = msg_uid eml in
  do attrId <- attributeId flagName
     q <- dbQuery "SELECT Value FROM MessageAttrs WHERE MessageId = ? AND AttributeId = ?"
          [uid, attrId]
     return $
       foldr (\entry acc ->
               case entry of
                 [DS.SQLInteger 0] -> acc
                 [DS.SQLInteger 1] -> True
                 _ -> error $ "strange result " ++ (show entry) ++ " from flag query uid " ++ (show uid) ++ " flag name " ++ flagName
                 )
       False
       q

clearFlag :: MessageFlag -> Message -> ImapServer ()
clearFlag MessageFlagSeen eml = clearSimpleFlag "harbinger.seen" eml
clearFlag MessageFlagRecent eml = clearSimpleFlag "harbinger.recent" eml
clearFlag MessageFlagDeleted eml =
  liftServer $ writeIORef (msg_deleted eml) False
  
clearSimpleFlag :: String -> Message -> ImapServer ()
clearSimpleFlag flagName eml =
  let uid = msg_uid eml in
  do attrId <- attributeId flagName
     ignore $
       dbQuery
       "DELETE FROM MessageAttrs WHERE MessageId = ? AND AttributeId = ?"
       [uid, attrId]
       
setFlag :: MessageFlag -> Message -> ImapServer ()
setFlag MessageFlagSeen eml =
  let uid = msg_uid eml in
  do attrId <- attributeId "harbinger.seen"
     ignore $
       dbQuery
       "UPDATE MessageAttrs SET Value = 1 WHERE MessageId = ? AND AttributeId = ?"
       [uid, attrId]
     nrChanges <- ImapServer $ \state ->
       do r <- DS.changes $ iss_database state
          return $ Right (state, r)
     if nrChanges == 0
       then ignore $ dbQuery "INSERT INTO MessageAttrs (MessageId, AttributeId, Value) VALUES (?, ?, 1)" [uid, attrId]
       else return ()
setFlag MessageFlagRecent _ = ImapServer $ \_ -> return $ Left $ ImapStopFailed "Cannot set recent flag"
setFlag MessageFlagDeleted eml =
  liftServer $ writeIORef (msg_deleted eml) True
  
grabMessageAttribute :: FetchAttribute -> Message -> ImapServer [(String, BS.ByteString)]
grabMessageAttribute attr msg =
  case attr of
    FetchAttrUid -> return [("UID", uidToByteString $ msg_uid msg)]
    FetchAttrFlags ->
      do flags <- liftM (map (msgFlagName . fst) . filter snd) $ mapM (\flg -> liftM ((,) flg) $ fetchMessageFlag flg msg) allMessageFlags
         return [("FLAGS", stringToByteString $ "(" ++ (intercalate " " flags) ++ ")")]
    FetchAttrInternalDate ->
      return $ case extractMessageHeader "date" (msg_email msg) of
        Nothing -> []
        Just v -> [("INTERNALDATE", stringToByteString $ renderQuotedString v)]
    FetchAttrRfc822Size -> return [("RFC822.SIZE", stringToByteString $ show $ BS.length $ eml_body $ msg_email msg)]
    FetchAttrBody peek sectionspec byterange ->
      do clearFlag MessageFlagRecent msg
         if not peek
           then setFlag MessageFlagSeen msg
           else return ()
         case (sectionspec, byterange) of
           ((Just (SectionMsgHeaderFields invert headers)), Nothing) ->
             return [("BODY",
                      stringToByteString $ "BODY[HEADER.FIELDS (" ++ (intercalate " " headers) ++ ")] " ++ (renderLiteralString $ extractMessageHeaders invert headers $ msg_email msg) ++ "\r\n")]
           (Nothing, Nothing) ->
             return [("UID", uidToByteString $ msg_uid msg),
                     ("BODY[]", renderLiteralByteString $ extractFullMessage $ msg_email msg)]
      
sendFetchResponse :: MsgSequenceNumber -> [(String, BS.ByteString)] -> ImapServer ()
sendFetchResponse (MsgSequenceNumber nr) attributes =
  let res = concatMap (\(a, b) -> [stringToByteString a,b]) attributes in
  sendResponseBs ResponseUntagged ResponseStateNone [] $
  BS.concat [stringToByteString $ show nr,
             stringToByteString " FETCH (",
             BS.intercalate (stringToByteString " ") res,
             stringToByteString ")"]
  
fetchMessage :: [FetchAttribute] -> MsgSequenceNumber -> ImapServer ()
fetchMessage attrs seqNr =
  do msg <- loadMessage seqNr
     results <- liftM concat $ mapM (flip grabMessageAttribute msg) attrs
     sendFetchResponse seqNr results

fetchMessageUid :: [FetchAttribute] -> MsgUid -> ImapServer ()
fetchMessageUid attrs uid =
  do msg <- loadMessageByUid uid
     results <- liftM concat $ mapM (flip grabMessageAttribute msg) attrs
     seqnr <- uidToSequenceNumber uid
     sendFetchResponse seqnr results
  
untilError :: ImapServer a -> ImapServer a
untilError server = ImapServer $ \state ->
  do r <- run_is server state
     case r of
       Left st -> return $ Left st
       Right (state', _) -> run_is (untilError server) state'

parserToServer :: ImapRequestParser a -> ImapServer a
parserToServer irp = ImapServer $ run_irp irp
       
processClient :: Handle -> IO ()
processClient clientHandle =
  runImapServer clientHandle $ do sendResponseOk ResponseUntagged [] "Hello"
                                  untilError (runWithErrs (parserToServer readCommand) >>= processCommand)
     
main :: IO ()
main =
  withSocketsDo $
  do listenSock <- listenOn $ PortNumber 5000
     forever $ do (clientHandle, clientHost, clientPort) <- accept listenSock
                  print $ "Accepted from " ++ clientHost ++ ", " ++ (show clientPort)
                  ignore $ forkIO $  finally (processClient clientHandle) (hClose clientHandle)
