module Email(Email(Email, eml_headers, eml_body),
             Header(Header)) where

import qualified Data.ByteString as BS

data Header = Header String String deriving Show
data Email = Email { eml_headers :: [Header], eml_body :: BS.ByteString } deriving Show

