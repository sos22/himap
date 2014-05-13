{- Thing to get headers into a canonical format for the database index. -}
module HeaderParse(parseHeader) where

import qualified Codec.Text.IConv as IC
import qualified Codec.Binary.Base64 as Base64
import qualified Codec.Binary.QuotedPrintable as QuotedPrintable
import Control.Monad
import qualified Data.ByteString as DB
import qualified Data.ByteString.Lazy as DBL
import Data.Char
import Data.List hiding (group)
import qualified Data.Text as DT
import Data.Time.Clock.POSIX
import Data.Time.Format
import qualified Data.Time.Git as DTG
import qualified Database.SQLite3 as DS
import Debug.Trace
import System.Locale

import Email

{- Less a parser and more a canonicaliser.  Maps from strings (usually
   header values) to lists of things under which the message should be indexed. -}
data ParserState = ParserState {ps_consumed :: Bool,
                                ps_rest :: String, 
                                ps_dropspace :: Bool}
                   deriving Show
data Parser a = Parser {
  run_parser :: ParserState -> [(a, ParserState)]
  }

instance Monad Parser where
  return x = Parser $ \s -> [(x, s)]
  fstM >>= sndF = Parser $ \state ->
    do (res1, state') <- run_parser fstM state
       run_parser (sndF res1) state'
  
everythingElse :: Parser String
everythingElse = Parser $ \state ->
  [(ps_rest state, state {ps_consumed = case ps_rest state of
                             "" -> ps_consumed state
                             _ -> False,
                          ps_rest = ""})]
  
alternatives :: [Parser a] -> Parser a
alternatives opts = Parser $ \state ->
  flip concatMap opts $ \o -> run_parser o state

alternatives' :: [(Parser a, b)] -> Parser b
alternatives' opts =
  alternatives $ map (\(o,r) -> o >> return r) opts

demaybe :: [Maybe a] -> [a]
demaybe [] = []
demaybe (Nothing:x) = demaybe x
demaybe ((Just x):xs) = x:(demaybe xs)

nonEmpty :: Parser a -> Parser a
nonEmpty parser =
  Parser $ \state -> filter (ps_consumed . snd) $
                     run_parser parser (state {ps_consumed = False})
         
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

skipSpace :: Parser a -> Parser a
skipSpace p =
  do oldDrop <- Parser $ \state -> [(ps_dropspace state,
                                     state {ps_dropspace = True})]
     r <- p
     Parser $ \state -> [((), state {ps_dropspace = oldDrop})]
     return r
     
{- invert a is a parser which matches iff a does not -}
invert :: Parser a -> Parser ()
invert p = Parser $ \state ->
  case run_parser p state of
    [] -> [((), state)]
    _ -> []

notFollowedBy :: Parser a -> Parser b -> Parser a
notFollowedBy a b = a `followedBy` (invert b)

optional :: Parser a -> Parser (Maybe a)
optional what = alternatives [ return Nothing,
                               liftM Just what ]
char :: Char -> Parser Char
char c =
  Parser $ \state ->
  case ps_rest state of
    "" -> []
    (cc:ccs) ->
      let nextState = state {ps_consumed = True,
                             ps_rest = ccs}
      in if cc `elem` " \r\t\n\f\v" && ps_dropspace state
         then run_parser (char c) nextState
         else if cc == c
              then [(c, nextState)]
              else []
charRange :: Int -> Int -> Parser Char
charRange start end =
  Parser $ \state ->
  case ps_rest state of
    [] -> []
    xx:xs ->
      let nextState = state {ps_consumed = True,
                             ps_rest = xs}
      in if xx `elem` " \r\t\n\f\v" && ps_dropspace state
         then run_parser (charRange start end) nextState
         else      
           if ord xx >= start && ord xx <= end
           then [(xx, nextState)]
           else []

eof :: Parser ()
eof =
  Parser $ \state -> case ps_rest state of
    "" -> [((), state)]
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

deEncode :: String -> String
deEncode "" = ""
deEncode ('=':'?':ss) =
  let token = many1 tokenChar
      sToBS :: String -> DB.ByteString
      sToBS = DB.pack . map (fromInteger . toInteger . ord)
      bsToS :: DB.ByteString -> String
      bsToS = map (chr . fromInteger . toInteger) . DB.unpack
      decodeBase64 :: String -> String
      decodeBase64 what =
        case Base64.decode $ sToBS what of
          Left _ -> what
          Right x -> bsToS x
      decodeQuotedPrintable what =
        case QuotedPrintable.decode $ sToBS what of
          Left _ -> what
          Right x -> bsToS x
      tokenChar = alternatives [charRange 33 33,
                                charRange 35 39,
                                charRange 42 43,
                                charRange 45 45,
                                charRange 47 57,
                                charRange 65 90,
                                charRange 92 92,
                                charRange 94 126]
      deEncode1 = do charset <- liftM (map toLower) token
                     _ <- char '?'
                     decoder <- alternatives' [(char 'q', decodeQuotedPrintable),
                                               (char 'Q', decodeQuotedPrintable),
                                               (char 'b', decodeBase64),
                                               (char 'B', decodeBase64)]
                     _ <- char '?'          
                     ec <- liftM decoder $ many1 $ alternatives [charRange 33 62,
                                                                 charRange 64 126]
                     _ <- char '?'
                     _ <- char '='
                     _ <- alternatives $ (map (\x -> char x >> return ()) " \t\r\n\v") ++ [return ()]
                     let r1 = IC.convertFuzzy IC.Discard charset "utf-8" $ DBL.pack $ map (fromInteger . toInteger . ord) ec
                     if DBL.null r1
                       then Parser $ \_ -> []
                       else return $ map (chr . fromInteger . toInteger) $ DBL.unpack r1
  in
  case run_parser deEncode1 $ ParserState { ps_consumed = False,
                                            ps_rest = ss,
                                            ps_dropspace = False} of
    [] -> '=':'?':(deEncode ss)
    ((s, ps):_) -> s ++ (deEncode $ ps_rest ps)
deEncode (x:xs) = x:(deEncode xs)  

runParser :: Show a => Parser a -> String -> Maybe a
runParser p what =
  let p' = (stripCFWS p) `followedBy` eof
  in
  case run_parser p' (ParserState {ps_consumed = False, 
                                   ps_rest = deEncode what,
                                   ps_dropspace = False}) of
    ((x, _):_) -> trace ((show what) ++ " -> " ++ (show x)) $
                  Just x
    _ -> Nothing

-- This is a fairly direct transcript of the grammar from RFC2822,
-- because it's easier to do that than to sit and thinnk about what it
-- all means.
msgId :: Parser [DS.SQLData]
msgId =
  alternatives [do descr <- liftM (maybe [] singleton) $ optional $ phrase `followedBy` skipCFWS
                   _ <- many1greedy $ char '<'
                   l <- skipSpace $ many1Sep (alternatives [noFoldQuote, noFoldLiteral, localPart, domainLiteral]) (char '@')
                   _ <- skipSpace $ many1greedy $ char '>'
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
  alternatives [liftM singleton noWsCtl,
                liftM singleton $ charRange 33 126,
                liftM singleton $ charRange 128 255,
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
  skipSpace $
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
dotAtomText =
  let sep = (many1 $ alternatives $ map char ".:;,")
  in liftM (intercalate ".") $ do r <- many1Sep atom (stripCFWS sep)
                                  _ <- optional sep
                                  return r

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
dtext = alternatives [noWsCtl, charRange 33 90, charRange 94 126, charRange 128 255]
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
                charRange 93 126,
                charRange 128 255] >> return ()
wSP :: Parser ()
wSP = (alternatives $ map char " \r\t\n\v") >> return ()
aText :: Parser Char
aText =
  alternatives [ charRange 33 33,
                 charRange 35 39,
                 charRange 42 43,
                 charRange 45 45,
                 charRange 47 57,
                 charRange 61 61,
                 charRange 63 63,
                 charRange 65 90,
                 charRange 94 126,
                 charRange 128 255]
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
                charRange 93 126,
                charRange 128 255]
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
                 charRange 14 127,
                 charRange 128 255]
phrase :: Parser String
phrase =
  liftM (intercalate " ") $
  many1Sep (alternatives [(many1greedy (alternatives [aText, char '.', char ',', char ':', char ';', char '@'])),
                          quotedString]) cFWS
obsQp :: Parser Char
obsQp = (char '\\') >> charRange 0 255

stringField :: Parser [String] -> Parser [DS.SQLData]
stringField = liftM (map $ DS.SQLText . DT.pack)

inReplyToParser :: Parser [DS.SQLData]
inReplyToParser =
  stripCFWS $ alternatives [liftM concat $ manySep msgId (nonEmpty skipCFWS),
                            do _ <- char '<'
                               _ <- char '>'
                               return [] ]
  
{- references is supposed to be space-separated, but some
   clients use commas.  Tolerate that -}
referencesParser :: Parser [DS.SQLData]
referencesParser =
  stripCFWS $ liftM concat $ manySep msgId (stripCFWS $ optional $ char ',')
  
date :: Parser [DS.SQLData]
date = do s <- alternatives [quotedString, everythingElse]
          alternatives [Parser $ \state -> case parseTime defaultTimeLocale rfc822DateFormat s of
                           Nothing -> []
                           Just d -> [([DS.SQLInteger $ round $ realToFrac $ utcTimeToPOSIXSeconds d], state)],
                         Parser $ \state -> case DTG.approxidate s of
                           Nothing -> []
                           Just d -> [([DS.SQLInteger $ fromInteger d], state)]]
            
-- mapping from RFC822 header name to (attribute name, attribute
-- parser) pairs.
parsers :: [(String, (String, Parser [DS.SQLData]))]
parsers = [("message-id", ("rfc822.Message-Id", msgId)),
           ("in-reply-to", ("rfc822.In-Reply-To", inReplyToParser)),
           ("references", ("rfc822.References", referencesParser)),
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
           ("date", ("rfc822.Date", date))]

parseHeader :: Header -> Either String [(String, DS.SQLData)]
parseHeader (Header name value) =
  case lookup (map toLower name) parsers of
    Just (fieldname, parser) ->
      trace ("Parse " ++ name ++ " ---> " ++ (show value)) $
      case runParser parser value of
        Nothing -> Left $ "cannot parse header " ++ name ++ ", value " ++ (show value)
        Just val -> Right $ [(fieldname, v) | v <- val]
    Nothing -> Right [] {- Not an indexed field -}