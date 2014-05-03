import Data.List
import Control.Monad.Writer.Lazy
import qualified Data.ByteString as BS
import qualified Database.SQLite3 as DS
import qualified Data.Text as DT
import Control.Exception.Base
import System.Exit

import Deliver
import Email
import Util

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
               "CREATE TABLE MailBoxes (Name STRING UNIQUE)",
               "INSERT INTO MailBoxes (Name) VALUES (\"INBOX\")",
               "CREATE UNIQUE INDEX AttributeRmap ON Attributes (Description)",
               "CREATE INDEX AttrRmap ON MessageAttrs (AttributeId, Value)",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Message-Id')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.In-Reply-To')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.References')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.From')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Sender')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Reply-To')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.To')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Cc')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Bcc')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Subject')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Comments')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Keywords')",
               "INSERT INTO Attributes (Description) VALUES ('rfc822.Date')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.seen')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.mailbox')",
               "INSERT INTO Attributes (Description) VALUES ('harbinger.recent')",
               "CREATE TABLE HarbingerVersion (Version INTEGER)",
               "INSERT INTO HarbingerVersion (Version) VALUES (1)",
               "END TRANSACTION"]
  in sequence_ stmts
     
loadAttributeTable :: DS.Database -> IO [(String, DS.SQLData)]
loadAttributeTable database =
  withStatement database (DT.pack "SELECT * FROM Attributes") worker
  where worker stmt = do r <- DS.step stmt
                         case r of
                           DS.Row ->
                             do v <- DS.columns stmt
                                case v of
                                  [attribId, DS.SQLText description] ->
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
     content <- BS.getContents
     case (runErrorable . parseEmail) content of
       Left errs -> do print $ byteStringToString content
                       putStrLn $ "Failed: " ++ (show errs) ++ "\n"
       Right parsed' ->
         do success <- fileEmail Nothing [] database attribs parsed'
            if success
              then exitSuccess
              else exitFailure

