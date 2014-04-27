{- Thing to get headers into a canonical format for the database index. -}
module HeaderParse(parseHeader) where

import Control.Monad
import Data.Char
import Data.List hiding (group)
import qualified Data.Text as DT
import qualified Database.SQLite3 as DS
import Debug.Trace

import Email

{- Less a parser and more a canonicaliser.  Maps from strings (usually
   header values) to lists of things under which the message should be indexed. -}
data Parser a = Parser { run_parser :: Bool -> String -> Either String (a, Bool, String) }

instance Monad Parser where
  return x = Parser $ \c r -> Right (x, c, r)
  fstM >>= sndF = Parser $ \c initialStr ->
    case run_parser fstM c initialStr of
      Left err -> Left err
      Right (res1, c', midStr) -> run_parser (sndF res1) c' midStr
  
alternatives :: [Parser a] -> Parser a
alternatives [] = Parser $ \_ _ -> Left "ran out of alternatives"
alternatives (a:as) = Parser $ \c str ->
  case run_parser a c str of
    Left _ -> run_parser (alternatives as) c str
    Right (res, c', leftover) -> Right (res, c', leftover)

nonEmpty :: String -> Parser a -> Parser a
nonEmpty msg parser =
  Parser $ \_ initStr ->
  case run_parser parser False initStr of
    Left err -> Left err
    Right (_, False, "") -> Left "nonEmpty empty string at end of file"
    Right (_, False, leftover) -> Left $ msg ++ " failed to consume any input at " ++ (show initStr) ++ ", leftover " ++ (show leftover)
    Right (res, True, leftover) -> Right (res, True, leftover)
    
many1 :: Parser a -> Parser [a]
many1 p =
  do one <- nonEmpty "many1" p
     rest <- alternatives [many1 p, return []]
     return $ one:rest
many :: Parser a -> Parser [a]
many p = alternatives [many1 p, return []]
many1Sep :: Parser a -> Parser b -> Parser [a]
many1Sep thing seperator =
  alternatives [do one <- nonEmpty "many1sep" (do x <- thing
                                                  _ <- seperator
                                                  return x)
                   rest <- many1Sep thing seperator
                   return $ one:rest,
                do one <- nonEmpty "many1sep singleton" thing
                   return [one]]
optional :: Parser a -> Parser (Maybe a)
optional what = alternatives [ liftM Just what,
                               return Nothing ]
char :: Char -> Parser Char
char c = Parser $ \_ x -> case x of
  "" -> Left $ "EOF looking for " ++ [c]
  (cc:ccs) | cc == c -> Right (c, True, ccs)
           | otherwise -> Left $ "Wanted " ++ [c] ++ ", got "++ x
charRange :: Int -> Int -> Parser Char
charRange start end = Parser $ \_ x ->
  case x of
    [] -> Left $ "EOF looking for >= " ++ (show start) ++ " <= " ++ (show end)
    xx:xs ->
      if ord xx >= start && ord xx <= end
      then Right (xx, True, xs)
      else Left $ "Wanted >= " ++ (show start) ++ " <= " ++ (show end) ++ ", got " ++ x

runParser :: Show a => Parser a -> String -> Either String a
runParser p what =
  case run_parser p False what of
    Right (x, _, "") -> trace ("Parsing " ++ what ++ " gave " ++ (show x)) $
                        Right x
    Right (_, _, leftovers) -> Left $ "Junk at end of field? " ++ (show leftovers)
    Left err -> Left $ "Error " ++ (show err) ++ " parsing " ++ (show what)

-- This is a fairly direct transcript of the grammar from RFC2822,
-- because it's easier to do that than to sit and thinnk about what it
-- all means.
msgId :: Parser DS.SQLData
msgId = do _ <- optional $ cFWS
           _ <- char '<'
           l <- idLeft
           _ <- char '@'
           r <- idRight
           _ <- char '>'
           _ <- optional $ cFWS
           return $ DS.SQLText $ DT.pack $ l ++ "@" ++ r
idLeft :: Parser String
idLeft = alternatives [dotAtomText, noFoldQuote, obsIdLeft]
idRight :: Parser String
idRight = alternatives [dotAtomText, noFoldLiteral, obsIdRight]
noFoldQuote :: Parser String
noFoldQuote =
  do _ <- char '"'
     _ <- optional $ char '\''
     r <- liftM concat $
          many (alternatives [liftM singleton qtext,
                              quotedPair])
     _ <- optional $ char '\''
     _ <- char '"'
     return r
noFoldLiteral :: Parser String
noFoldLiteral =
  do _ <- char '['
     r <- liftM concat $
          many (alternatives [liftM singleton dtext, quotedPair])
     _ <- char ']'
     return r
mailboxList :: Parser [String]
mailboxList =
  alternatives [liftM concat $ many1Sep mailbox (char ','),
                obsMboxList]
addressList :: Parser [String]
addressList =
  alternatives [liftM concat $ many1Sep address (char ','),
                obsAddrList,
                return []]
unstructured :: Parser [String]
unstructured =
  do r <- many $ do a <- optional fWS
                    b <- utext
                    return $ case a of
                      Nothing -> b
                      Just _ -> ' ':b
     _ <- optional fWS
     return [concat r]
utext :: Parser String
utext =
  alternatives [liftM (\x -> [x]) noWsCtl,
                liftM (\x -> [x]) $ charRange 33 126,
                obsUtext]
phrase :: Parser String
phrase =
  alternatives [liftM (intercalate " ") $ many1 word,
                obsPhrase]
word :: Parser String
word = alternatives [atom, quotedString]
address :: Parser [String]
address = alternatives [mailbox, group]
mailbox :: Parser [String]
mailbox = alternatives [nameAddr, liftM (\x -> [x]) addrSpec]
nameAddr :: Parser [String]
nameAddr =
  do dn <- optional displayName
     aa <- angleAddr
     return $ case dn of
       Just dn' -> [dn',aa]
       Nothing -> [aa]
angleAddr :: Parser String
angleAddr =
  alternatives [do _ <- optional cFWS
                   as <- do _ <- char '<'
                            r <- addrSpec
                            _ <- char '>'
                            return r
                   _ <- optional cFWS
                   return as,
                obsAngleAddr]
group :: Parser [String]
group =
  do dn <- displayName
     _ <- char ':'
     ml <- optional $ alternatives [mailboxList, cFWS >> return []]
     _ <- char ';'
     _ <- optional cFWS
     return $ case ml of
       Just x -> dn:x
       Nothing -> [dn]
displayName :: Parser String
displayName = phrase
addrSpec :: Parser String
addrSpec =
  do l <- localPart
     _ <- char '@'
     d <- domain
     return $ l ++ "@" ++ d
localPart :: Parser String
localPart = alternatives [dotAtom, quotedString, obsLocalPart]
domain :: Parser String
domain = alternatives [dotAtom, domainLiteral, obsDomain]
domainLiteral :: Parser String
domainLiteral =
  do _ <- optional cFWS
     _ <- char '['
     r <- many $ do a <- optional fWS
                    r <- dcontent
                    return $ case a of
                      Nothing -> r
                      Just _ -> ' ':r
     _ <- optional fWS
     _ <- char ']'
     _ <- optional cFWS
     return $ concat r
dcontent :: Parser String
dcontent = alternatives [liftM singleton dtext, quotedPair]
dtext :: Parser Char
dtext = alternatives [noWsCtl, charRange 33 90, charRange 94 126]
noWsCtl :: Parser Char
noWsCtl =
  alternatives [charRange 1 8,
                charRange 11 12,
                charRange 14 31,
                charRange 127 127]
singleton :: a -> [a]
singleton x = [x]
text :: Parser String
text =
  alternatives [liftM singleton $ charRange 1 9,
                liftM singleton $ charRange 11 12,
                liftM singleton $ charRange 14 127,
                obsText]
quotedPair :: Parser String
quotedPair =
  alternatives [(char '\\') >> text,
                liftM singleton obsQp]
fWS :: Parser ()
fWS = alternatives [many1 wSP >> return (), obsFWS] >> return ()
cFWS :: Parser ()
cFWS =
  do _ <- many (optional fWS >> comment)
     alternatives [(optional fWS) >> comment, fWS]
     return ()
comment :: Parser ()
comment =
  do _ <- char '('
     _ <- many (fWS >> cContent)
     fWS
     _ <- char ')'
     return ()
cContent :: Parser ()
cContent = alternatives [cText, quotedPair >> return (), comment] >> return ()
cText :: Parser ()
cText =
  alternatives [noWsCtl,
                charRange 33 39,
                charRange 41 91,
                charRange 93 126] >> return ()
wSP :: Parser ()
wSP = (alternatives $ map char " \r\t\n\v") >> return ()
dotAtom :: Parser String
dotAtom =
  do _ <- optional cFWS
     r <- dotAtomText
     _ <- optional cFWS
     return r
dotAtomText :: Parser String
dotAtomText = liftM (intercalate ".") $ many1Sep (many1 aText) (char '.')
aText :: Parser Char
aText =
  alternatives $
  map char
  "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM!#$%&\'*+-/=?^_`{|}~"
atom :: Parser String
atom =
  do _ <- optional cFWS
     r <- many1 aText
     _ <- optional cFWS
     return r
quotedString :: Parser String
quotedString =
  do _ <- optional cFWS
     _ <- char '"'
     r <- many $ do a <- optional fWS
                    b <- qcontent
                    return $ case a of
                      Nothing -> b
                      Just _ -> ' ':b
     _ <- optional fWS
     _ <- char '"'
     _ <- optional cFWS
     return $ concat r
qcontent :: Parser String
qcontent = alternatives [liftM singleton qtext, quotedPair]
qtext :: Parser Char
qtext =
  alternatives [noWsCtl,
                charRange 33 33,
                charRange 35 91,
                charRange 93 126]
obsIdLeft :: Parser String
obsIdLeft = localPart
obsIdRight :: Parser String
obsIdRight = domain
obsMboxList :: Parser [String]
obsMboxList =
  do r <- liftM concat $ many1 $ do rr <- optional mailbox
                                    _ <- optional cFWS
                                    _ <- char ','
                                    _ <- optional cFWS
                                    return $ maybe [] id rr
     l <- optional mailbox
     return $ case l of
       Nothing -> r
       Just ls -> ls ++ r
obsAddrList :: Parser [String]
obsAddrList =
  do r <- many1 $ do rr <- optional address
                     _ <- optional cFWS
                     _ <- char ','
                     _ <- optional cFWS
                     return $ maybe [] id rr
     l <- optional address
     return $ (concat r) ++ (maybe [] id l)
obsAngleAddr :: Parser String
obsAngleAddr =
  do _ <- optional cFWS
     _ <- char '<'
     _ <- optional obsRoute
     a <- addrSpec
     _ <- char '>'
     _ <- optional cFWS
     return a
obsRoute :: Parser String
obsRoute =
  do _ <- optional cFWS
     dl <- obsDomainList
     _ <- char ':'
     _ <- optional cFWS
     return dl
obsDomainList :: Parser String
obsDomainList =
  do _ <- char '@'
     d <- domain
     ds <- many $ do _ <- many $ alternatives [cFWS, char ',' >> return ()]
                     _ <- optional cFWS
                     _ <- char '@'
                     domain
     return $ '@':(intercalate "@" (d:ds))
obsDomain :: Parser String
obsDomain = liftM (intercalate ".") $ many1Sep atom (char '.')
obsLocalPart :: Parser String
obsLocalPart = liftM (intercalate ".") $ many1Sep word (char '.')
obsUtext :: Parser String
obsUtext = obsText
obsText :: Parser String
obsText =
  do _ <- many $ char '\r'
     _ <- many $ char '\n'
     liftM concat $ many $ do rr <- obsChar
                              a <- many $ char '\r'
                              b <- many $ char '\n'
                              return $ case (a, b) of
                                ("", "") -> [rr]
                                _ -> rr:" "
obsChar :: Parser Char
obsChar =
  alternatives [ charRange 0 9,
                 charRange 11 12,
                 charRange 14 127]
obsPhrase :: Parser String
obsPhrase =
  do w <- word
     r <- many $ alternatives [word,
                               liftM singleton $ char '.',
                               cFWS >> return ""]
     return $ intercalate " " $ w:r
obsQp :: Parser Char
obsQp = (char '\\') >> charRange 0 127
obsFWS :: Parser ()
obsFWS =
  (many1 wSP) >> (many (char '\r' >> char '\n' >> many1 wSP)) >> return ()

stringField :: Parser [String] -> Parser [DS.SQLData]
stringField = liftM (map $ DS.SQLText . DT.pack)

-- mapping from RFC822 header name to (attribute name, attribute
-- parser) pairs.
parsers :: [(String, (String, Parser [DS.SQLData]))]
parsers = [("in-reply-to", ("rfc822.In-Reply-To", many1 msgId)),
           ("references", ("rfc822.References", many1 msgId)),
           ("from", ("rfc822.From", stringField mailboxList)),
           ("sender", ("rfc822.Sender", stringField addressList)),
           ("reply-to", ("rfc822.Reply-To", stringField addressList)),
           ("to", ("rfc822.To", stringField addressList)),
           ("cc", ("rfc822.Cc", stringField addressList)),
           ("bcc", ("rfc822.Bcc", stringField addressList)),
           ("subject", ("rfc822.Subject", stringField unstructured)),
           ("comments", ("rfc822.Comments", stringField unstructured)),
           ("keywords", ("rfc822.Keywords",
                         stringField $ many1Sep phrase (char ','))),
           ("date", ("rfc822.Date", stringField $ unstructured))]

parseHeader :: Header -> Either String [(String, DS.SQLData)]
parseHeader (Header name value) =
  case lookup (map toLower name) parsers of
    Just (fieldname, parser) ->
      case runParser parser value of
        Left err -> Left $ "cannot parse header " ++ name ++ ", value " ++ (show value) ++ ": " ++ err
        Right val -> Right $ [(fieldname, v) | v <- val]
    Nothing -> Right [] {- Not an indexed field -}