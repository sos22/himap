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
data Parser a = Parser { run_parser :: Bool -> String -> [(a, Bool, String)] }

instance Monad Parser where
  return x = Parser $ \c r -> [(x, c, r)]
  fstM >>= sndF = Parser $ \c initialStr ->
    do (res1, consumed1, leftover1) <- run_parser fstM c initialStr
       run_parser (sndF res1) consumed1 leftover1
  
alternatives :: [Parser a] -> Parser a
alternatives opts = Parser $ \c str ->
  flip concatMap opts $ \o -> run_parser o c str

demaybe :: [Maybe a] -> [a]
demaybe [] = []
demaybe (Nothing:x) = demaybe x
demaybe ((Just x):xs) = x:(demaybe xs)

nonEmpty :: Parser a -> Parser a
nonEmpty parser =
  Parser $ \_ initStr -> filter (\(_, x, _) -> x) $ run_parser parser False initStr
         
many1 :: Parser a -> Parser [a]
many1 p =
  do one <- nonEmpty p
     rest <- alternatives [many1 p, return []]
     return $ one:rest
many1greedy :: Parser a -> Parser [a]
many1greedy p = (many1 p) `notFollowedBy` p
many :: Parser a -> Parser [a]
many p = alternatives [many1 p, return []]
many1Sep :: Parser a -> Parser b -> Parser [a]
many1Sep thing seperator =
  alternatives [do one <- nonEmpty $ (do x <- thing
                                         _ <- seperator
                                         return x)
                   rest <- many1Sep thing seperator
                   return $ one:rest,
                do one <- thing
                   return [one]]
manySep :: Parser a -> Parser b -> Parser [a]
manySep thing seperator =
  alternatives [many1Sep thing seperator, return []]
                
followedBy :: Parser a -> Parser b -> Parser a
followedBy a b =
  do r <- a
     _ <- b
     return r

{- invert a is a parser which matches iff a does not -}
invert :: Parser a -> Parser ()
invert p = Parser $ \c str ->
  case run_parser p False str of
    [] -> [((), c, str)]
    _ -> []

notFollowedBy :: Parser a -> Parser b -> Parser a
notFollowedBy a b = a `followedBy` (invert b)

optional :: Parser a -> Parser (Maybe a)
optional what = alternatives [ return Nothing,
                               liftM Just what ]
char :: Char -> Parser Char
char c = Parser $ \_ x -> case x of
  "" -> []
  (cc:ccs) | cc == c -> [(c, True, ccs)]
           | otherwise -> []
charRange :: Int -> Int -> Parser Char
charRange start end = Parser $ \_ x ->
  case x of
    [] -> []
    xx:xs ->
      if ord xx >= start && ord xx <= end
      then [(xx, True, xs)]
      else []

eof :: Parser ()
eof = Parser $ \c x -> case x of
  "" -> [((), c, "")]
  _ -> []

skipCFWS :: Parser ()
skipCFWS = liftM (const ()) $ optional cFWS
stripCFWS :: Parser a -> Parser a
stripCFWS p =
  do skipCFWS
     r <- p
     skipCFWS
     return r
skipFWS :: Parser ()
skipFWS = liftM (const ()) $ optional fWS
stripFWS :: Parser a -> Parser a
stripFWS p =
  do skipFWS
     r <- p
     skipFWS
     return r
runParser :: Show a => Parser a -> String -> Maybe a
runParser p what =
  let p' = do pp <- p
              eof
              return pp
  in
  case run_parser p' False what of
    ((x, _, ""):_) -> trace (what ++ " -> " ++ (show x)) $
                      Just x
    _ -> Nothing

-- This is a fairly direct transcript of the grammar from RFC2822,
-- because it's easier to do that than to sit and thinnk about what it
-- all means.
msgId :: Parser [DS.SQLData]
msgId =
  alternatives [do descr <- liftM (maybe [] singleton) $ optional $ phrase `followedBy` skipCFWS
                   _ <- many1greedy $ char '<'
                   l <- many1Sep (alternatives [noFoldQuote, noFoldLiteral, localPart, domainLiteral]) (char '@')
                   _ <- many1greedy $ char '>'
                   return $ map (DS.SQLText . DT.pack) $ descr ++ [intercalate "@" l],
                do l <- many1Sep (alternatives [noFoldQuote, noFoldLiteral, localPart, domainLiteral]) (char '@')
                   return [DS.SQLText $ DT.pack $ intercalate "@" l]]
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
address :: Parser [String]
address = alternatives [mailbox, group, liftM singleton phrase]
mailbox :: Parser [String]
mailbox = alternatives [nameAddr, liftM (\x -> [x]) addrSpec]
nameAddr :: Parser [String]
nameAddr =
  stripCFWS $
  do dn <- optional $ do r <- phrase
                         skipCFWS
                         return r
     aa <- angleAddr
     return $ case dn of
       Just dn' -> [dn',aa]
       Nothing -> [aa]
angleAddr :: Parser String
angleAddr = obsAngleAddr
group :: Parser [String]
group =
  do dn <- stripCFWS phrase
     _ <- char ':'
     ml <- optional $ alternatives [mailboxList, cFWS >> return []]
     _ <- char ';'
     skipCFWS
     return $ case ml of
       Just x -> dn:x
       Nothing -> [dn]
addrSpec :: Parser String
addrSpec =
  stripCFWS $
  do l <- localPart
     _ <- stripCFWS $ char '@'
     d <- domain
     return $ l ++ "@" ++ d

localPart :: Parser String
localPart =
  alternatives [quotedString, dotAtomText]

atom :: Parser String
atom = many1 aText
dotAtomText :: Parser String
dotAtomText = liftM (intercalate ".") $ many1Sep atom (stripCFWS $ char '.')

domain :: Parser String
domain = alternatives [domainLiteral, dotAtomText]
domainLiteral :: Parser String
domainLiteral =
  do _ <- char '['
     r <- many $ do a <- optional fWS
                    r <- dcontent
                    return $ case a of
                      Nothing -> r
                      Just _ -> ' ':r
     _ <- optional fWS
     _ <- char ']'
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
quotedPair :: Parser String
quotedPair =
  liftM singleton obsQp
fWS :: Parser ()
fWS = many1greedy wSP >> return ()
cFWS :: Parser ()
cFWS = (many1 $ alternatives [comment, fWS]) >> return ()
comment :: Parser ()
comment =
  do _ <- char '('
     _ <- many $ alternatives [fWS, cContent]
     _ <- char ')'
     return ()
cContent :: Parser ()
cContent = alternatives [cText, quotedPair >> return (), comment] >> return ()
cText :: Parser ()
cText =
  alternatives [noWsCtl,
                charRange 33 39,
                charRange 42 91,
                charRange 93 126] >> return ()
wSP :: Parser ()
wSP = (alternatives $ map char " \r\t\n\v") >> return ()
aText :: Parser Char
aText =
  alternatives $
  map char
  "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM!#$%&\'*+-/=?^_`{|}~"
quotedString :: Parser String
quotedString =
  do _ <- char '"'
     r <- stripFWS $ liftM (intercalate " ") $ manySep (liftM concat $ many1 qcontent) fWS
     _ <- char '"'
     return $ r
qcontent :: Parser String
qcontent = alternatives [liftM singleton qtext, quotedPair]
qtext :: Parser Char
qtext =
  alternatives [noWsCtl,
                charRange 33 33,
                charRange 35 91,
                charRange 93 126]
obsMboxList :: Parser [String]
obsMboxList =
  liftM (concat . demaybe) $ many1Sep (optional mailbox) (stripCFWS $ char ',')
obsAddrList :: Parser [String]
obsAddrList =
  liftM (concat . demaybe) $ many1Sep (optional address) (stripCFWS $ char ',')
obsAngleAddr :: Parser String
obsAngleAddr =
  do _ <- char '<'
     _ <- optional obsRoute
     a <- addrSpec
     _ <- char '>'
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
     d <- stripCFWS domain
     ds <- many $ do _ <- many $ alternatives [cFWS, char ',' >> return ()]
                     _ <- optional cFWS
                     _ <- char '@'
                     skipCFWS
                     r <- domain
                     skipCFWS
                     return r
     return $ '@':(intercalate "@" (d:ds))
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
phrase :: Parser String
phrase =
  liftM (intercalate " ") $
  many1Sep (alternatives [(many1greedy (alternatives [aText, char '.', char ',', char ':', char ';', char '@'])),
                          quotedString]) cFWS
obsQp :: Parser Char
obsQp = (char '\\') >> charRange 0 127

stringField :: Parser [String] -> Parser [DS.SQLData]
stringField = liftM (map $ DS.SQLText . DT.pack)

inReplyToParser :: Parser [DS.SQLData]
inReplyToParser =
  stripCFWS $ alternatives [liftM concat $ manySep msgId skipCFWS,
                            do _ <- char '<'
                               _ <- char '>'
                               return [] ]
  
-- mapping from RFC822 header name to (attribute name, attribute
-- parser) pairs.
parsers :: [(String, (String, Parser [DS.SQLData]))]
parsers = [("in-reply-to", ("rfc822.In-Reply-To", inReplyToParser)),
           {- references is supposed to be space-separated, but some
              clients use commas.  Tolerate that -}
           ("references", ("rfc822.References", stripCFWS $ liftM concat $ manySep msgId (stripCFWS $ optional $ char ','))),
           ("from", ("rfc822.From", stringField mailboxList)),
           ("sender", ("rfc822.Sender", stringField addressList)),
           ("reply-to", ("rfc822.Reply-To", stringField addressList)),
           ("to", ("rfc822.To", stringField addressList)),
           ("cc", ("rfc822.Cc", stringField addressList)),
           ("bcc", ("rfc822.Bcc", stringField addressList)),
           ("subject", ("rfc822.Subject", stringField unstructured)),
           ("comments", ("rfc822.Comments", stringField unstructured)),
           ("keywords", ("rfc822.Keywords",
                         stringField $ many1Sep phrase (stripCFWS $ char ','))),
           ("date", ("rfc822.Date", stringField $ unstructured))]

parseHeader :: Header -> Either String [(String, DS.SQLData)]
parseHeader (Header name value) =
  case lookup (map toLower name) parsers of
    Just (fieldname, parser) ->
      trace ("Parse " ++ name ++ " ---> " ++ (show value)) $
      case runParser parser value of
        Nothing -> Left $ "cannot parse header " ++ name ++ ", value " ++ (show value)
        Just val -> Right $ [(fieldname, v) | v <- val]
    Nothing -> Right [] {- Not an indexed field -}