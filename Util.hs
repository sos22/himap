module Util(stringToByteString,
            byteStringToString) where

import qualified Data.ByteString as BS
import Data.Char

stringToByteString :: String -> BS.ByteString
stringToByteString = BS.pack . map (fromInteger . toInteger . ord)
byteStringToString :: BS.ByteString -> String
byteStringToString = map (chr.fromInteger.toInteger) . BS.unpack

