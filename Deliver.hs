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
import Network.BSD
import System.Directory
import System.IO
import System.Locale
import System.Random

import Email
import HeaderParse
import Util

import ImapServer

data Journal = Journal { journal_handle :: Handle,
                         journal_path :: FilePath }
data JournalEntry = JournalStart String Int64
                  | JournalAddSymlink String String
                  | JournalRegisterMessage String Int64
                    deriving (Show, Read)

openCreate :: FilePath -> IO (Maybe Handle)
openCreate path = do exists <- doesFileExist path
                     if exists
                       then return Nothing
                       else liftM Just $ openFile path WriteMode

journalWrite :: Journal -> JournalEntry -> IO ()
journalWrite journal je =
  let h = journal_handle journal in
  do hPutStrLn h $ show je
     hFlush h
  
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

dbExec :: DS.Database -> String -> [DS.SQLData] -> IO ()
dbExec database what values =
  withStatement database (DT.pack what) $ \stmt ->
  do DS.bind stmt values
     r <- DS.step stmt
     case r of
       DS.Done -> return ()
       _ -> error $ "dbExec " ++ what ++ " didn't work?"
     return ()
addDbRow :: DS.Database -> String -> [DS.SQLData] -> IO ()
addDbRow database table values =
  dbExec database ("INSERT INTO " ++ table ++ " VALUES (" ++ (intercalate ", " (map (const "?") values)) ++ ")") values
     
addAttribute :: DS.Database -> [(String, DS.SQLData)] -> MsgUid -> String -> DS.SQLData -> IO ()
addAttribute database attribs msgId attribName value =
  case lookup attribName attribs of
    Nothing -> error $ "Bad attribute " ++ (show attribName)
    Just attrib -> addDbRow database "MessageAttrs" [msgId, attrib, value]
    
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
                           rrr <- DS.step stmt'
                           case rrr of
                             DS.Row -> error "update to allocate message ID didn't work?"
                             DS.Done -> return ()
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

getHeader :: String -> Email -> Maybe String
getHeader name eml =
  lookup (map toLower name) $ map (\(Header x y) -> (map toLower x,y)) $ eml_headers eml

findHeader :: String -> Email -> [DS.SQLData]
findHeader name eml =
  let n = map toLower name in
  case find (\(Header x _) -> n == map toLower x) $ eml_headers eml of
    Nothing -> []
    Just x -> case parseHeader x of
      Left _ -> []
      Right y -> map snd y
  
dupeCheck :: [(String, DS.SQLData)] -> DS.Database -> Email -> IO (Maybe MsgUid)
dupeCheck attribs database eml =
  let mMsgId = head $ findHeader "Message-Id" eml
      mSubject =
        case findHeader "Subject" eml of
          [] -> DS.SQLText $ DT.pack "<<<no subject>>>"
          (x:_) -> x
      mFrom =
        case findHeader "From" eml of
          [] -> case findHeader "Sender" eml of
            [] -> DS.SQLText $ DT.pack "<<<no sender>>>"
            (x:_) -> x
          (x:_) -> x
  in
  case (lookup "rfc822.Subject" attribs,
        lookup "rfc822.From" attribs,
        lookup "rfc822.Message-Id" attribs) of
    (Just subject, Just from, Just msgId) ->
      withStatement database
      (DT.pack
       "SELECT msgId.MessageId FROM MessageAttrs AS msgId JOIN MessageAttrs AS subject JOIN MessageAttrs AS frm ON msgId.AttributeId = ? AND msgId.Value = ? AND msgId.MessageId = subject.MessageId AND subject.AttributeId = ? AND subject.Value = ? AND frm.AttributeId = ? AND frm.Value = ? AND frm.MessageId = subject.MessageId") $ \stmt ->
      do DS.bind stmt [msgId, mMsgId, subject, mSubject, from, mFrom]
         r <- DS.step stmt
         case r of
           DS.Row -> do v <- DS.columns stmt
                        return $ case v of
                          [rr] -> Just rr
                          _ -> error $ "Unexpected result " ++ (show v) ++ " from dupe check query"
           _ -> return Nothing
    _ -> error "harbinger DB not correctly initialised"                      
    
addMessageToPool :: [(String, DS.SQLData)] -> Journal -> DS.Database -> Email -> IO (String, MsgUid)
addMessageToPool attribs journal database email =
  do existing <- dupeCheck attribs database email
     case existing of
       Just x ->
         do r <- withStatement database (DT.pack "SELECT Location FROM Messages WHERE MessageId = ?") $ \stmt ->
              do DS.bind stmt [x]
                 r <- DS.step stmt
                 case r of
                   DS.Row -> do v <- DS.columns stmt
                                return $ case v of
                                  [DS.SQLText t] -> DT.unpack t
                                  _ -> error $ "unexpected result " ++ (show v) ++ " getting location of existing message"
                   _ -> error "cannot get location of existing message"
            return (r, x)
       Nothing ->
         do msgDbId <- allocMsgDbId database
            now <- Data.Time.getCurrentTime
            let prefix = Data.Time.Format.formatTime System.Locale.defaultTimeLocale "harbinger/byDate/%F/" now
            createDirectoryIfMissing True prefix
            let fname = prefix ++ "/" ++ (show msgDbId)
            journalWrite journal $ JournalStart fname msgDbId
            hh <- openCreate fname
            case hh of
              Nothing -> error $ "Failed to generate unique message ID! (" ++ prefix ++ ", " ++ (show msgDbId) ++ ")"
              Just hh' ->
                do BS.hPut hh' $ flattenEmail email
                   hClose hh'
                   addDbRow database "Messages" [DS.SQLInteger msgDbId, DS.SQLText $ DT.pack fname]
                   return (fname, DS.SQLInteger msgDbId)

purgeMessage :: DS.Database -> String -> DS.SQLData -> IO ()
purgeMessage database fname dbId =
  do removeFile fname
     dbExec database "DELETE FROM Messages WHERE MessageId = ?" [dbId]
     dbExec database "DELETE FROM MessageAttrs WHERE MessageId = ?" [dbId]
     
fileEmail :: Maybe String -> [MessageFlag] -> DS.Database -> [(String,DS.SQLData)] -> Email -> IO Bool
fileEmail mailbox flags database attribs eml =  
  do eml' <- ensureMessageId eml
     j <- openMessageJournal
     (fname, msgDbId) <- addMessageToPool attribs j database eml'
     failed <- transactional database $
       do failed <- liftM or $ flip mapM (eml_headers eml') $ \header ->
            case parseHeader header of
              Left err -> hPutStrLn stderr err >> return True
              Right dbattribs ->
                (flip mapM_ dbattribs $ \(tableName, value) ->
                  addAttribute database attribs msgDbId tableName value) >> return False
          if failed
            then hPutStrLn stderr "failed to add message to index"
            else do flip mapM_ (MessageFlagRecent:flags) $ \flag ->
                      addAttribute database attribs msgDbId (msgFlagDbName flag) (DS.SQLInteger 1)
                    case mailbox of
                      Just mailbox' -> addAttribute database attribs msgDbId "harbinger.mailbox" (DS.SQLText $ DT.pack mailbox')
                      Nothing -> return ()
          return failed
     if failed
       then purgeMessage database fname msgDbId
       else return ()
     hClose $ journal_handle j
     {- XXX Need to do an fsync here XXX -}
     removeFile $ journal_path j
     return $ not failed
