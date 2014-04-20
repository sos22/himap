module Util(stringToByteString,
            byteStringToString,
            runErrorable,
            Errorable) where

import Control.Monad.Writer.Lazy

import qualified Data.ByteString as BS
import Data.Char

type Errorable = Writer [String]

runErrorable :: Errorable a -> Either [String] a
runErrorable x = case runWriter x of
  (l, []) -> Right l
  (_, r) -> Left r

stringToByteString :: String -> BS.ByteString
stringToByteString = BS.pack . map (fromInteger . toInteger . ord)
byteStringToString :: BS.ByteString -> String
byteStringToString = map (chr.fromInteger.toInteger) . BS.unpack

