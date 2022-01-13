#include "indexed_row_table.h"
#include <cstring>
namespace bytedance_db_project
{
  IndexedRowTable::IndexedRowTable(int32_t index_column)
  {
    index_column_ = index_column;
  }

  void IndexedRowTable::Load(BaseDataLoader *loader)
  {
    // TODO: Implement this!
    num_cols_ = loader->GetNumCols();
    auto rows = loader->GetRows();
    num_rows_ = rows.size();
    rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_);
      char *temp = new char[FIXED_FIELD_LEN];
      std::memcpy(temp, cur_row, FIXED_FIELD_LEN);
      int32_t indexdata = temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
      index_[indexdata].push_back(row_id);
    }
  }

  int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id)
  {
    // TODO: Implement this!
    int32_t result;
    int32_t start =
        row_id * num_cols_ * FIXED_FIELD_LEN + col_id * FIXED_FIELD_LEN;
    result = rows_[start] + (rows_[start + 1] << 8) + (rows_[start + 2] << 16) +
             (rows_[start + 3] << 24);
    return result;
  }

  void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                    int32_t field)
  {
    // TODO: Implement this!
    int32_t oldvalue = GetIntField(row_id, col_id);
    char *temp = new char[4];
    temp[0] = (field & 0XFF);
    temp[1] = (field & 0XFF00);
    temp[2] = (field & 0XFF0000);
    temp[3] = (field & 0XFF000000);
    memcpy(rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN, temp,
           FIXED_FIELD_LEN);

    if (col_id == 0)
    {
      auto it = index_[oldvalue].begin();
      while (it != index_[oldvalue].end() && (*it != row_id))
      {
        it++;
      }
      if (it != index_[oldvalue].end())
      {
        index_[oldvalue].erase(it);
        if (index_[oldvalue].size() == 0)
        {
          index_.erase(oldvalue);
        }
      }
      index_[field].push_back(row_id);
    }
  }

  int64_t IndexedRowTable::ColumnSum()
  {
    // TODO: Implement this!
    int64_t result = 0;
    for (int32_t i = 0; i < num_rows_; i++)
    {
      char *temp = new char[FIXED_FIELD_LEN];
      memcpy(temp, rows_ + i * FIXED_FIELD_LEN * num_cols_, FIXED_FIELD_LEN);
      result += temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
    }

    return result;
    return 0;
  }

  int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                               int32_t threshold2)
  {
    // TODO: Implement this!
    int64_t result = 0;
    for (int32_t i = 0; i < num_rows_; i++)
    {
      if (GetIntField(i, 1) > threshold1 && GetIntField(i, 2) < threshold2)
      {
        result += GetIntField(i, 0);
      }
    }
    return result;
  }

  int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold)
  {
    // TODO: Implement this!
    int64_t result = 0;
    for (auto it = index_.begin(); it != index_.end(); it++)
    {
      if (it->first > threshold)
      {
        for (int32_t i = 0; i < (int32_t)it->second.size(); i++)
        {
          for (int32_t j = 0; j < num_cols_; j++)
          {
            result += GetIntField(it->second[i], j);
          }
        }
      }
    }
    return result;
  }

  int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold)
  {
    // TODO: Implement this!
    int64_t count = 0;
    for (auto it = index_.begin(); it != index_.end(); it++)
    {
      if (it->first < threshold)
      {
        for (int32_t i = 0; i < (int32_t)it->second.size(); i++)
        {
          int32_t reslut = GetIntField(it->second[i], 2) + GetIntField(it->second[i], 3);
          PutIntField(it->second[i], 3, reslut);
          count++;
        }
      }
    }
    return count;
  }
} // namespace bytedance_db_project