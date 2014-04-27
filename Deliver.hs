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
import System.Posix.Files
import System.Random

import Email
import HeaderParse
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
  do hPutStrLn h $ case je of
       JournalStart msgId -> "start " ++ msgId
       JournalAddSymlink msgId name -> "symlink " ++ msgId ++ " " ++ name
       JournalRegisterMessage msgId msgDbId -> "register " ++ msgId ++ " " ++ (show msgDbId)
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

addDbRow :: DS.Database -> String -> [DS.SQLData] -> IO ()
addDbRow database table values =
  withStatement database (DT.pack $ "INSERT INTO " ++ table ++ " VALUES (" ++ (intercalate ", " (map (const "?") values)) ++ ")") $ \stmt ->
  do DS.bind stmt values
     DS.step stmt
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

removeHeader :: String -> Email -> Email
removeHeader hdr eml = eml { eml_headers = filter (\(Header name _) -> (map toLower name) /= (map toLower hdr)) $ eml_headers eml }

deMaybe :: Maybe a -> a
deMaybe (Just x) = x
deMaybe Nothing = error "Maybe wasn't?"

sanitiseForPath :: String -> String
sanitiseForPath = filter $ flip elem "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM,^@%-+_:"

fileEmail :: String -> [MessageFlag] -> DS.Database -> [(String,DS.SQLData)] -> Email -> IO Bool
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
     failed <- liftM or $ flip mapM (eml_headers eml'') $ \header ->
       case parseHeader header of
         Left err -> hPutStrLn stderr err >> return True
         Right dbattribs ->
           (flip mapM_ dbattribs $ \(tableName, value) ->
             addAttribute database attribs msgDbId tableName value) >> return False
     if failed
       then do hPutStrLn stderr "failed to add message to index"
               return False
       else do flip mapM_ (MessageFlagRecent:flags) $ \flag ->
                 addAttribute database attribs msgDbId (msgFlagDbName flag) (DS.SQLInteger 1)
               addAttribute database attribs msgDbId "harbinger.mailbox" (DS.SQLText $ DT.pack mailbox)
               hClose $ journal_handle j
               {- XXX Need to do an fsync here XXX -}
               removeFile $ journal_path j
               return True
