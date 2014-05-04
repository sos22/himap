module ImapParser(ImapRequestParser(run_irp),
                  readCommand) where

import Control.Monad
import qualified Data.ByteString as BS
import Data.Char
import Data.IORef
import qualified Database.SQLite3 as DS
import Data.Word
import Debug.Trace
import qualified OpenSSL.Session as SSL

import Util

import ImapServer

data ImapRequestParser a = ImapRequestParser { run_irp :: ImapServerState -> IO (Either ImapStopReason (ImapServerState, a)) }
instance Monad ImapRequestParser where
  return x = ImapRequestParser $ \state -> return $ Right (state, x)
  first >>= secondF = ImapRequestParser $ \state ->
    do firstRes <- run_irp first state
       case firstRes of
         Left r -> return $ Left r
         Right (newState, firstRes') ->
           run_irp (secondF firstRes') newState
  
lookaheadByte :: ImapRequestParser Word8
lookaheadByte = ImapRequestParser worker
  where worker state =
          do inbuf <- readIORef $ iss_inbuf state
             if iss_inbuf_idx state >= BS.length inbuf
               then do nextChunk <- case iss_handle state of
                         Left handle -> BS.hGetSome handle 4096
                         Right ssl -> SSL.read ssl 4096
                       trace ("grabbed chunk " ++ (byteStringToString nextChunk)) $
                         if BS.length nextChunk == 0
                         then return $ Left ImapStopFinished
                         else do modifyIORef (iss_inbuf state) (flip BS.append nextChunk)
                                 worker state
               else return $ Right (state, BS.index inbuf (iss_inbuf_idx state))

readByte :: ImapRequestParser Word8
readByte = lookaheadByte >>= \res ->
  ImapRequestParser $ \state -> return $ Right (state {iss_inbuf_idx = (iss_inbuf_idx state) + 1},
                                                res)
                                     
readCountedBytes :: Int -> ImapRequestParser [Word8]
readCountedBytes 0 = return []
readCountedBytes x =
  do c <- readByte
     c' <- readCountedBytes (x - 1)
     return $ c:c'

readCountedByteString :: Int -> ImapRequestParser BS.ByteString
readCountedByteString amt = ImapRequestParser worker
  where worker state =
          do inbuf <- readIORef $ iss_inbuf state
             if amt + iss_inbuf_idx state >= BS.length inbuf
               then do nextChunk <- case iss_handle state of
                         Left handle -> BS.hGetSome handle (amt - iss_inbuf_idx state)
                         Right ssl -> SSL.read ssl 4096
                       if BS.length nextChunk == 0
                         then return $ Left ImapStopBacktrack
                         else do modifyIORef (iss_inbuf state) (flip BS.append nextChunk)
                                 worker state
               else return $ Right (state {iss_inbuf_idx = amt + iss_inbuf_idx state},
                                    BS.take amt $ BS.drop (iss_inbuf_idx state) inbuf)

readCountedChars :: Int -> ImapRequestParser String
readCountedChars n = liftM (map (chr.fromInteger.toInteger)) $ readCountedBytes n

readChar :: ImapRequestParser Char
readChar = liftM (chr.fromInteger.toInteger) readByte

lookaheadChar :: ImapRequestParser Char
lookaheadChar = liftM (chr.fromInteger.toInteger) lookaheadByte

parseBacktrack :: ImapRequestParser a
parseBacktrack = ImapRequestParser $ \_ -> return $ Left ImapStopBacktrack

alternates :: [ImapRequestParser a] -> ImapRequestParser a
alternates options = ImapRequestParser $ worker options
  where worker [] _ = return $ Left $ ImapStopBacktrack
        worker (opt1:opts) state =
          do res1 <- run_irp opt1 state
             case res1 of
               Left ImapStopBacktrack -> worker opts state
               Left err -> return $ Left err
               Right res2 -> return $ Right res2
optional :: ImapRequestParser a -> ImapRequestParser (Maybe a)        
optional what = alternates [liftM Just what, return Nothing]

imapParserFlush :: ImapRequestParser ()
imapParserFlush = ImapRequestParser $ \state -> do modifyIORef (iss_inbuf state) (BS.drop (iss_inbuf_idx state))
                                                   return $ Right (state { iss_inbuf_idx = 0 }, ())

skipToEOL :: ImapRequestParser String
skipToEOL = do c <- readChar
               if c == '\r'
                 then state1
                 else liftM ((:) c) skipToEOL
            where state1 = do c <- readChar
                              if c == '\n'
                                then return ""
                                else if c == '\r'
                                     then state1
                                     else liftM ((:) c) skipToEOL
                                          
{- I don't understand what IMAP continuations are for, other than
adding an unnecessary RTT and making everything more of a pain to
implement.  There's an equivalent protocol which is like IMAP except
that the client never waits for continuations and the server never
sends them, and that protocol is, as far as I can tell, strictly
superior, but since some clients depend on receiving continuations at
certain times we have to be able to send them. -}
sendContinuation :: ImapRequestParser ()
sendContinuation = ImapRequestParser $ run_is $ (queueResponse "+ Go ahead\r\n") >> finishResponse
  
readCommand :: ImapRequestParser (ResponseTag, ImapCommand)
readCommand =
  do t <- liftM ResponseTagged readTag
     requireChar ' '
     cmd <- alternates [alternates $ map (\(name, x) -> do requireString name
                                                           r <- x
                                                           requireEOL
                                                           return r) commandParsers,
                        do l <- skipToEOL
                           return $ ImapCommandBad l
                       ]
     imapParserFlush
     return (t, cmd)
  where readTag = alternates [do c <- readChar
                                 if c == ' '
                                   then parseBacktrack
                                   else liftM ((:) c) readTag,
                              return ""]
        requireChar c = do c' <- readChar
                           if c == c'
                             then return ()
                             else parseBacktrack
        requireString = sequence_ . (map requireChar)
        requireEOL = requireString "\r\n"
        parseAstring = alternates [parseQuoted, parseLiteral, parseMany1 parseAstringChar]
        parseQuoted = requireChar '"' >> worker
          where worker = do c <- readChar
                            case c of
                              '"' -> return ""
                              '\\' -> do c' <- readChar
                                         liftM ((:) c') worker
                              _ -> liftM ((:) c) worker
        parseNumber :: Integral a => ImapRequestParser a
        parseNumber =
          do c <- lookaheadChar
             if not $ c `elem` "1234567890"
               then parseBacktrack
               else worker 0
               where worker acc =
                       do c <- lookaheadChar
                          if not (c `elem` "1234567890")
                            then return acc
                            else do _ <- readChar
                                    worker $ (acc * 10) + (fromInteger $ toInteger $ digitToInt c)
        parseLiteral = do requireChar '{'
                          cnt <- parseNumber
                          requireString "}\r\n"
                          sendContinuation
                          readCountedChars cnt
        parseLiteralByteString = do requireChar '{'
                                    cnt <- parseNumber
                                    requireString "}\r\n"
                                    sendContinuation
                                    readCountedByteString cnt
        parseMany1 what = do r <- what
                             alternates [do res <- parseMany1 what
                                            return $ r:res,
                                         return [r]]
        parseMany1Sep sep elm = do r <- elm
                                   alternates [ do _ <- sep
                                                   res <- parseMany1Sep sep elm
                                                   return $ r:res,
                                                return [r] ]
        parseManySep sep elm =                                        
          alternates [ parseMany1Sep sep elm,
                       return [] ]
        parseAstringChar = do c <- readChar
                              if (c `elem` "(){ %*\"\\") || (ord c < 32)
                                then parseBacktrack
                                else return c
        parseFlag =
          let worker =
                alternates [do c <- lookaheadChar
                               if c `elem` "1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM-_\\"
                                 then do _ <- readChar
                                         rest <- worker
                                         return $ c:rest
                                 else parseBacktrack,
                            return ""] in
          do c <- worker
             case c of
               "\\Seen" -> return MessageFlagSeen
               "\\Recent" -> return MessageFlagRecent
               "\\Deleted" -> return MessageFlagDeleted
               _ -> parseBacktrack
        
        commandParsers = [("NOOP", return ImapNoop),
                          ("EXPUNGE", return ImapExpunge),
                          ("CLOSE", return ImapClose),
                          ("LOGOUT", return ImapLogout),
                          ("CAPABILITY", return ImapCapability),
                          ("LOGIN", do requireChar ' '
                                       username <- parseAstring
                                       requireChar ' '
                                       password <- parseAstring
                                       return $ ImapLogin username password),
                          ("LIST", do requireChar ' '
                                      referenceName <- parseMailbox
                                      requireChar ' '
                                      listMailbox <- parseListMailbox
                                      return $ ImapList referenceName listMailbox),
                          ("SELECT", do requireChar ' '
                                        liftM ImapSelect parseMailbox),
                          ("FETCH", do requireChar ' '
                                       seqs <- parseSequenceSet
                                       requireChar ' '
                                       attrs <- parseFetchAttributes
                                       return $ ImapFetch seqs attrs),
                          ("UID FETCH ", do uids <- parseUidSet
                                            requireChar ' '
                                            attrs <- parseFetchAttributes
                                            return $ ImapFetchUid uids attrs),
                          ("UID STORE ", do uids <- parseUidSet
                                            trace ("store uids " ++ (show uids)) $ requireChar ' '
                                            mode <- optional $ alternates [ requireString "+" >> return True,
                                                                            requireString "-" >> return False]
                                            trace ("mode " ++ (show mode)) $ requireString "FLAGS"
                                            silent <- alternates [ requireString ".SILENT" >> return True,
                                                                   return False ]
                                            requireString " "
                                            flags <- trace ("silent " ++ (show silent)) $
                                                     alternates [ do trace "C1" $ requireString "("
                                                                     r <- trace "C2" $ parseManySep (requireString " ") parseFlag
                                                                     trace ("C3 " ++ (show r)) $ requireString ")"
                                                                     trace "C4" $ return r,
                                                                  parseMany1Sep (requireString " ") parseFlag ]
                                            return $ ImapStoreUid uids mode silent flags),
                          ("UID COPY ", do uids <- parseUidSet
                                           requireChar ' '
                                           mbox <- parseMailbox
                                           return $ ImapCopyUid uids mbox),
                          ("STATUS ", do mbox <- parseMailbox
                                         requireString " ("
                                         flags <- parseMany1Sep (requireString " ") parseStatusItem
                                         requireChar ')'
                                         return $ ImapStatus mbox flags),
                          ("APPEND ", do mbox <- parseMailbox
                                         flags <- liftM (maybe [] id) $ optional $ do requireString " ("
                                                                                      r <- parseManySep (requireString " ") parseFlag
                                                                                      requireChar ')'
                                                                                      return r
                                         datetime <- optional $ do requireChar ' '
                                                                   parseQuoted
                                         requireChar ' '
                                         msg <- parseLiteralByteString
                                         return $ ImapAppend mbox flags datetime msg),
                          ("STARTTLS", return ImapStartTls)]
        parseFetchAttributes = alternates [do requireString "ALL"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size,
                                                      FetchAttrEnvelope],
                                           do requireString "FAST"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size],
                                           do requireString "FULL"
                                              return [FetchAttrFlags,
                                                      FetchAttrInternalDate,
                                                      FetchAttrRfc822Size,
                                                      FetchAttrEnvelope,
                                                      FetchAttrBody False Nothing Nothing],
                                           do requireChar '('
                                              r <- parseMany1Sep (requireChar ' ') parseFetchAttr
                                              requireChar ')'
                                              return r,
                                           liftM (\x -> [x]) parseFetchAttr]
        parseStatusItem = alternates $ flip map allStatusItems (\x -> (requireString $ statusItemName x) >> return x) 
        parseSequenceSet = liftM concat $ parseMany1Sep (requireChar ',') $ do n <- parseSeqNumber
                                                                               alternates [ do requireChar ':'
                                                                                               r <- parseSeqNumber
                                                                                               return [n..r],
                                                                                            return [n]]
        parseUidSet = liftM concat $ parseMany1Sep (requireChar ',') $ do n <- parseUid
                                                                          m <- optional $ do requireChar ':'
                                                                                             parseUid
                                                                          return $ maybe [n] (uidRange n) m
        parseSeqNumber = alternates [liftM MsgSequenceNumber parseNumber,
                                     (requireChar '*' >> getMaxSeqNumber)]
        getMaxSeqNumber = ImapRequestParser $ \state -> return $ Right (state, MsgSequenceNumber $ (length $ iss_messages state) + 1)
        parseUid = liftM DS.SQLInteger parseNumber
        parseSection = do requireChar '['
                          r <- optional parseSectionSpec
                          requireChar ']'
                          return r
        parseSectionSpec = alternates [parseSectionMsgText,
                                       do part <- parseSectionPart
                                          rest <- optional $ (requireChar '.' >> parseSectionText)
                                          return $ SectionSubPart part rest]
        parseSectionMsgText = alternates [(requireString "TEXT" >> return SectionMsgText),
                                          do requireString "HEADER.FIELDS"
                                             invert <- alternates [requireString "NOT" >> return True,
                                                                   return False]
                                             requireChar ' '
                                             headers <- parseHeaderList
                                             return $ SectionMsgHeaderFields invert headers,
                                          (requireString "HEADER" >> return SectionMsgHeader)]
        parseSectionPart = liftM MimeSectionPath $ parseMany1Sep (requireChar '.') parseNumber
        parseSectionText = alternates [parseSectionMsgText, (requireString "MIME" >> return SectionMsgMime)]
        parseHeaderList = do requireChar '('
                             r <- parseMany1Sep (requireChar ' ') parseHeaderFldName
                             requireChar ')'
                             return r
        parseHeaderFldName = parseAstring
        parseFetchAttr = alternates [(requireString "ENVELOPE" >> return FetchAttrEnvelope),
                                     (requireString "FLAGS" >> return FetchAttrFlags),
                                     (requireString "INTERNALDATE" >> return FetchAttrInternalDate),
                                     (requireString "RFC822.HEADER" >> return FetchAttrRfc822Header),
                                     (requireString "RFC822.SIZE" >> return FetchAttrRfc822Size),
                                     (requireString "RFC822.TEXT" >> return FetchAttrRfc822Text),
                                     (requireString "RFC822" >> return FetchAttrRfc822),
                                     (requireString "UID" >> return FetchAttrUid),
                                     (requireString "BODYSTRUCTURE" >> return FetchAttrBodyStructure),
                                     (requireString "BODY" >>
                                      do peek <- alternates [(requireString ".PEEK" >> return True),
                                                             return False]
                                         section <- liftM join $ optional parseSection
                                         byteRange <- optional $ do requireChar '<'
                                                                    l <- parseNumber
                                                                    requireChar '.'
                                                                    r <- parseNumber
                                                                    requireChar '>'
                                                                    return $ ByteRange l r
                                         return $ FetchAttrBody peek section byteRange)]
        parseMailbox = do s <- parseAstring
                          return $ if "inbox" == map toLower s 
                                   then "INBOX"
                                   else s
        parseListMailbox = alternates [parseLiteral,
                                       parseQuoted,
                                       parseMany1 parseListChar]
        parseListChar = do c <- readChar
                           if (c `elem` "(){ \\\"") || (ord c < 32)
                             then parseBacktrack
                             else return c

uidRange :: MsgUid -> MsgUid -> [MsgUid]
uidRange (DS.SQLInteger i) (DS.SQLInteger j) = map DS.SQLInteger [i..j]
uidRange a b = error $ "bad uid range (" ++ (show a) ++ " to " ++ (show b) ++ ")"

