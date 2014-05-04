module Util(stringToByteString,
            byteStringToString,
            dbExec,
            dbQuery,
            hashPassword,
            runErrorable,
            Errorable) where

import qualified Data.ByteString as BS
import Data.Char
import qualified Data.Text as DT
import qualified Database.SQLite3 as DS
import Control.Exception
import Control.Monad.Writer.Lazy
import Crypto.PasswordStore

type Errorable = Writer [String]

runErrorable :: Errorable a -> Either [String] a
runErrorable x = case runWriter x of
  (l, []) -> Right l
  (_, r) -> Left r

stringToByteString :: String -> BS.ByteString
stringToByteString = BS.pack . map (fromInteger . toInteger . ord)
byteStringToString :: BS.ByteString -> String
byteStringToString = map (chr.fromInteger.toInteger) . BS.unpack

dbQuery :: DS.Database -> String -> [DS.SQLData] -> IO [[DS.SQLData]]
dbQuery db what binders =   
  let fetchAllRows stmt =
        do r <- DS.step stmt
           case r of
             DS.Done -> return []
             DS.Row ->
               do v <- DS.columns stmt
                  liftM ((:) v) $ fetchAllRows stmt
  in do prepped <- DS.prepare db $ DT.pack what
        (DS.bind prepped binders >> fetchAllRows prepped) `finally` (DS.finalize prepped)

dbExec :: DS.Database -> String -> [DS.SQLData] -> IO ()
dbExec database what values =
  do r <- dbQuery database what values
     case r of
       [] -> return ()
       _ -> error $ "db exec returned rows " ++ (show r)

hashPassword :: String -> IO DS.SQLData
hashPassword what =
  liftM (DS.SQLText . DT.pack . byteStringToString) $ makePassword (stringToByteString what) 12
