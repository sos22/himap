import Network
import qualified Data.ByteString as BS
import System.IO
import Control.Concurrent
import Control.Exception
import Data.Char

ignore :: IO a -> IO ()
ignore = flip (>>) $ return ()

forever :: IO () -> IO ()
forever = ignore . sequence . repeat

processClient :: Handle -> IO ()
processClient clientHandle =
  do BS.hPut clientHandle $ BS.pack $ map (fromInteger . toInteger. ord) "HELLO!\r\n"
     rcvd <- BS.hGetSome clientHandle 128
     BS.hPut clientHandle rcvd
     
main :: IO ()
main =
  withSocketsDo $
  do listenSock <- listenOn $ PortNumber 5000
     forever $ do (clientHandle, clientHost, clientPort) <- accept listenSock
                  print $ "Accepted from " ++ clientHost ++ ", " ++ (show clientPort)
                  ignore $ forkIO $  finally (hSetBuffering clientHandle NoBuffering >> processClient clientHandle) (hClose clientHandle)
