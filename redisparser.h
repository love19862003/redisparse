/********************************************************************

  Filename:   redisparser

  Description:redisparser

  Version:  1.0
  Created:  7:1:2016   11:49
  Revison:  none
  Compiler: gcc vc

  Author:   wufan, love19862003@163.com

  Organization:
*********************************************************************/
#ifndef __FANNY_redisparser_H__
#define __FANNY_redisparser_H__
#include <vector>
#include <boost/lexical_cast.hpp>


namespace RedisParseSpace{

  const std::string  REDIS_LBR("\r\n");
  const std::string  REDIS_STATUS_REPLY_OK("OK");
  const std::string  REDIS_PREFIX_STATUS_REPLY_ERROR("-ERR ");
  const std::string  REDIS_PREFIX_STATUS_REPLY_ERR_C("-");
  const std::string  REDIS_PREFIX_STATUS_REPLY_VALUE("+");
  const std::string  REDIS_PREFIX_SINGLE_BULK_REPLY("$");
  const std::string  REDIS_PREFIX_MULTI_BULK_REPLY("*");
  const std::string  REDIS_PREFIX_INT_REPLY(":");
  const std::string  REDIS_PREFIX_LBR("\n");
  const std::string  REDIS_WHITESPACE(" \f\n\r\t\v");
  const std::string  REDIS_MISSING("**missing-key**");


  enum reply_t : unsigned int {
    no_reply,
    status_code_reply,
    error_reply,
    int_reply,
    bulk_reply,
    multi_bulk_reply
  };

  inline reply_t getReplyType(const std::string& str) {
    if(0 == str.find_first_of(REDIS_PREFIX_STATUS_REPLY_ERR_C)) {
      return  error_reply;
    }
    if(0 == str.find_first_of(REDIS_PREFIX_STATUS_REPLY_VALUE)) {
      return  status_code_reply;
    }
    if(0 == str.find_first_of(REDIS_PREFIX_SINGLE_BULK_REPLY)) {
      return  bulk_reply;
    }
    if(0 == str.find_first_of(REDIS_PREFIX_INT_REPLY)) {
      return  int_reply;
    }
    if(0 == str.find_first_of(REDIS_PREFIX_MULTI_BULK_REPLY)) {
      return  multi_bulk_reply;
    }
    return no_reply;
  }

  inline std::string & rtrim(std::string & str, const std::string & ws = REDIS_WHITESPACE) {
    std::string::size_type pos = str.find_last_not_of(ws);
    str.erase(pos + 1);
    return str;
  }

   class RedisParse
   {
   public:
     inline bool isDone()const { return m_done; }
     inline reply_t type() const { return m_type; }
     inline bool isError() const { return  error_reply == type(); }
     inline bool isStatus() const { return  status_code_reply == type(); }
     inline bool isInteger() const { return  int_reply == type(); }
     inline bool isBulk() const { return  bulk_reply == type(); }
     inline bool isMultiBulk() const { return  multi_bulk_reply == type(); }

     void debug() const{
       static const char* debugtype[multi_bulk_reply + 1] = {
         "no_reply", 
         "status_code_reply",
         "error_reply",
         "int_reply", 
         "bulk_reply",
         "multi_bulk_reply",
       };
       
       if (isDone()){
         std::cout << "redis result type :" << debugtype[m_type] << std::endl;
         for(auto& str : m_recvList) {
           std::cout << "redis :" << str << std::endl;
         }
       } else {
         std::cout << " redis data not done" << std::endl;
       }
     }

     bool parse(const char* buf, size_t size, size_t& readPos) {
       if (m_done){ return true;}
       if(0 == size) { return true; }
       assert(readPos < size);
       if(no_reply == m_type && !parseType(buf, size, readPos)) { return false;}
       if(!m_done) {parseBule(buf, size, readPos);}
       return m_done;
     }
   private:
     bool parseType(const char* buf, size_t size, size_t& readPos) {
       std::string content;
       if(!readLine(buf, size, readPos, content)) { return false; }
       m_type = getReplyType(content);
       switch(m_type) {
       case no_reply: {
         assert(false);  // error
         m_done = true;
       }break;
       case status_code_reply:{
         m_recvList.push_back(content.substr(1));  //"+"
         m_done = true;
       }break;
       case error_reply:{
         m_recvList.push_back(content.substr(4));  //"-ERR "
         m_done = true;
       }break;
       case int_reply: {
         m_recvList.push_back(content.substr(1)); //":"
         m_done = true;
       }break;
       case bulk_reply: {
         m_tempMaxLen = 0;
         int bulkLen = boost::lexical_cast<int>(content.substr(1));   //"$"
         if(-1 == bulkLen) {
           m_recvList.push_back(REDIS_MISSING);
           m_done = true;
         } else { 
           m_tempMaxLen = bulkLen + 2;   // "CRLF"
           m_done = false; 
         }
       }break;
       case multi_bulk_reply:{
         m_multiCount = 0;
         int count = boost::lexical_cast<int>(content.substr(1));  //"*"
         if(-1 == count) {
           m_done = true;
         } else {
           m_multiCount = count;
           m_done = false;
         }
       }break;
       default:
         break;
       }
       return true;
     }

     bool parseBule(const char* buf, size_t size, size_t& readPos) {
       if(bulk_reply == m_type) {
         std::string content;
         if(readLen(buf, size, readPos, m_tempMaxLen, content)) {
           content.erase(m_tempMaxLen - 2); // "CRLF"
           m_recvList.push_back(content);
           m_done = true;
           return true;
         }
         return false;
       }else if(multi_bulk_reply ==  m_type) {
         do 
         {
           std::string content;
           if (!readLine(buf, size, readPos, content)){ return false;}
           m_tempMaxLen = 0;
           const std::string& ssub = content.substr(1);   //"$"
           int bulkLen = boost::lexical_cast<int>(ssub);
           if(-1 == bulkLen) {
             m_recvList.push_back(REDIS_MISSING);
           } else {
             m_tempMaxLen = bulkLen + 2; // "CRLF"
             content.clear();
             if(!readLen(buf, size, readPos, m_tempMaxLen, content)) { return false; }
             content.erase(m_tempMaxLen - 2);  // "CRLF"
             m_recvList.push_back(content);
           }

           if(m_multiCount == m_recvList.size()) {
             m_done = true;
             break;
           } 
         } while (m_recvList.size() != m_multiCount);

         return m_multiCount == m_recvList.size();
       } else {
         return true;
       }
     }
     bool readLine(const char* buf, size_t size, size_t& readPos, std::string& result) {
       size_t toRead = size -  readPos;
       if(toRead <= 0) { return false; }
       const char * eol = static_cast<const char*>(memchr(buf + readPos, REDIS_PREFIX_LBR[0], toRead));
       if(eol) {
         toRead = eol - buf + 1;
         m_temp.append(buf + readPos, toRead - readPos);
         readPos = toRead;
         result = rtrim(m_temp, REDIS_LBR);
         m_temp.clear();
         return true;
       }
       return false;
     }
     bool readLen(const char* buf, size_t size, size_t& readPos, size_t len, std::string& result) {
       size_t toRead = size - readPos;
       if(toRead <= 0) { return false; }
       size_t need = len - m_temp.length();
       bool ok = false;
       if(need <= toRead) { toRead = need; ok = true; }
       m_temp.append(buf + readPos, toRead);
       readPos += toRead;
       if(ok) {
         result = m_temp;
         m_temp.clear();
         return true;
       }
       return false;
     }

   private:
     reply_t m_type = no_reply;
     size_t m_multiCount = 0;
     size_t m_tempMaxLen = 0;
     std::string m_temp;
     std::vector<std::string> m_recvList;
     bool m_done = false;
   private:
//      RedisParse(const RedisParse&) = delete;
//      RedisParse& operator = (const RedisParse&) = delete;
//      RedisParse& operator = (RedisParse&&) = delete;
   };

}

#endif