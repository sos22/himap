module ImapServer(ImapServerState(..),
                  Message(..),
                  ImapStopReason(..),
                  ResponseTag(..),
                  ImapCommand(..),
                  MessageFlag(..),
                  FetchAttribute(..),
                  MsgSequenceNumber(..),
                  SectionSpec(..),
                  MimeSectionPath(..),
                  ByteRange(..),
                  Mbox(..),
                  MsgUid,
                  ImapServer(..),
                  msgFlagName,
                  msgFlagDbName,
                  allMessageFlags,
                  StatusItem(..),
                  allStatusItems,
                  statusItemName,
                  queueResponse,
                  queueResponseBs,
                  finishResponse
                  ) where

import qualified Data.ByteString as BS
import Data.IORef
import qualified Data.Text as DT
import qualified Database.SQLite3 as DS
import Debug.Trace
import qualified OpenSSL.Session as SSL
import System.IO

import Email
import MailFilter
import Util

type MsgUid = DS.SQLData

data Message = Message { msg_email :: Email,
                         msg_uid :: DS.SQLData,
                         msg_deleted :: IORef Bool }

data Mbox = MboxPhysical String
          | MboxLogical MailFilter
            deriving Show
                     
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


data ImapServerState = ImapServerState { iss_handle :: Either Handle SSL.SSL,
                                         iss_outgoing_response :: [BS.ByteString],
                                         iss_inbuf :: IORef BS.ByteString,
                                         iss_inbuf_idx :: Int,
                                         iss_messages :: [Either MsgUid Message],
                                         iss_database :: DS.Database,
                                         iss_attributes :: [(String, DS.SQLData)],
                                         iss_selected_mbox :: Maybe Mbox,
                                         iss_mailboxes :: [DT.Text],
                                         iss_logged_in :: Bool
                                         }

data ResponseTag = ResponseUntagged
                 | ResponseTagged String
                   deriving Show

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

data MsgSequenceNumber = MsgSequenceNumber Int
                         deriving (Show)
instance Enum MsgSequenceNumber where
  toEnum = MsgSequenceNumber
  fromEnum (MsgSequenceNumber x) = x
     

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

data MessageFlag = MessageFlagSeen
                 | MessageFlagRecent
                 | MessageFlagDeleted
                   deriving (Show, Eq)
allMessageFlags :: [MessageFlag]
allMessageFlags = [MessageFlagSeen, MessageFlagRecent, MessageFlagDeleted]
msgFlagName :: MessageFlag -> String
msgFlagName MessageFlagSeen = "\\Seen"
msgFlagName MessageFlagRecent = "\\Recent"
msgFlagName MessageFlagDeleted = "\\Deleted"
msgFlagDbName :: MessageFlag -> String
msgFlagDbName MessageFlagSeen = "harbinger.seen"
msgFlagDbName MessageFlagRecent = "harbinger.recent"
msgFlagDbName MessageFlagDeleted = error "cannot set deleted flag in database"

data StatusItem = StatusItemMessages
                | StatusItemRecent
                | StatusItemUidNext
                | StatusItemUidValidity
                | StatusItemUnseen
                  deriving Show
allStatusItems :: [StatusItem]
allStatusItems = [StatusItemMessages,
                  StatusItemRecent,
                  StatusItemUidNext,
                  StatusItemUidValidity,
                  StatusItemUnseen]
statusItemName :: StatusItem -> String
statusItemName StatusItemMessages = "MESSAGES"
statusItemName StatusItemRecent = "RECENT"
statusItemName StatusItemUidNext = "UIDNEXT"
statusItemName StatusItemUidValidity = "UIDVALIDITY"
statusItemName StatusItemUnseen = "UNSEEN"

data ImapCommand = ImapNoop
                 | ImapCapability
                 | ImapLogin String String
                 | ImapList String String
                 | ImapSelect String
                 | ImapFetch [MsgSequenceNumber] [FetchAttribute]
                 | ImapFetchUid [MsgUid] [FetchAttribute]
                 | ImapStoreUid [MsgUid] (Maybe Bool) Bool [MessageFlag]
                 | ImapCopyUid [MsgUid] String
                 | ImapStatus String [StatusItem]
                 | ImapAppend String [MessageFlag] (Maybe String) BS.ByteString
                 | ImapExpunge
                 | ImapClose
                 | ImapLogout
                 | ImapStartTls
                 | ImapCommandBad String
                   deriving Show

queueResponseBs :: BS.ByteString -> ImapServer ()
queueResponseBs what = ImapServer $ \state ->
  return $ Right (state {iss_outgoing_response = what:(iss_outgoing_response state)},
                  ())
  
queueResponse :: String -> ImapServer ()
queueResponse = queueResponseBs . stringToByteString

finishResponse :: ImapServer ()
finishResponse = ImapServer $ \isState ->
  let (sender, flusher) =
        case iss_handle isState of
          Left handle ->
            (sequence_ . map (BS.hPut handle) . reverse, hFlush handle)
          Right ssl ->
            (SSL.write ssl . BS.concat . reverse, return ())
  in do sender $ map (\x -> trace ("sending " ++ (byteStringToString x)) x) $ iss_outgoing_response isState
        flusher
        return $ Right (isState {iss_outgoing_response = []}, ())
