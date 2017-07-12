/**********************************************
* Copyright (c) 2016 Huawei Technologies Co., Ltd. All rights reserved.
* 
* File name:    index_store_mock.h
* Author: 
* Date:         2017/07/10
* Version:      1.0
* Description:
* 
************************************************/
#ifndef INDEX_STORE_MOCK_H_
#define INDEX_STORE_MOCK_H_
#include "gmock/gmock.h"
#include "common/index_store.h"

class MockIndexStore : public IndexStore {
 public:
  MOCK_METHOD0(db_open,
      int());
  MOCK_METHOD0(db_close,
      int());
  MOCK_METHOD2(db_put,
      int(const std::string& key, const std::string& value));
  MOCK_METHOD1(db_get,
      std::string(const std::string& key));
  MOCK_METHOD1(db_del,
      int(const std::string& key));
  MOCK_METHOD1(db_key_not_exist,
      bool(const std::string& key));
  class MockIteratorInf : public IteratorInf {
     public:
      MOCK_METHOD0(seek_to_first,
          int());
      MOCK_METHOD0(seek_to_last,
          int());
      MOCK_METHOD1(seek_to_first,
          int(const std::string& prefix));
      MOCK_METHOD1(seek_to_last,
          int(const std::string& prefix));
      MOCK_METHOD0(valid,
          bool());
      MOCK_METHOD0(prev,
          int());
      MOCK_METHOD0(next,
          int());
      MOCK_METHOD0(key,
          std::string());
      MOCK_METHOD0(value,
          std::string());
  };

  MOCK_METHOD0(db_iterator,
      IndexIterator());
  MOCK_METHOD0(fetch_transaction,
      Transaction());
  MOCK_METHOD1(submit_transaction,
      int(Transaction t));
};

#endif
