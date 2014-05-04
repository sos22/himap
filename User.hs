import qualified Database.SQLite3 as DS
import qualified Data.Text as DT
import System.Environment
import System.Exit

import Util

exists :: DS.Database -> String -> IO Bool
exists database username =         
  do r <- dbQuery database "SELECT COUNT(*) FROM Users WHERE Username = ?" [DS.SQLText $ DT.pack username]
     return $ case r of
       [[DS.SQLInteger 0]] -> False
       [[DS.SQLInteger 1]] -> True
       _ -> error $ "existing user query produced unexpected result " ++ (show r)

ifM :: Monad m => m Bool -> m a -> m a -> m a
ifM a b c =
  do a' <- a
     if a' then b else c
     
main :: IO ()
main =
  do args <- getArgs
     database <- DS.open $ DT.pack "harbinger.db"
     case args of
       ["CREATE", username, password] ->
         ifM (exists database username)
         (putStrLn "already exists" >> exitFailure)
         (do pwd <- hashPassword password
             dbExec database "INSERT INTO Users VALUES (?,?)" [DS.SQLText $ DT.pack username, pwd])
       ["LIST"] ->
         do r <- dbQuery database "SELECT Username FROM Users" []
            flip mapM_ r $ \rr ->
              case rr of
                [DS.SQLText uname] -> putStrLn $ DT.unpack uname
                _ -> putStrLn ("username table has odd entry " ++ (show rr)) >> exitFailure
       ["REMOVE", username] ->
         ifM (exists database username)
         (dbExec database "DELETE FROM Users WHERE Username = ?" [DS.SQLText $ DT.pack username])
         (putStrLn "no such user" >> exitFailure)
       ["PASSWORD", username, password] ->
         ifM (exists database username)
         (do pwd <- hashPassword password
             dbExec database "UPDATE Users SET Password = ? WHERE Username = ?" [pwd, DS.SQLText $ DT.pack username])
         (putStrLn "no such user" >> exitFailure)
