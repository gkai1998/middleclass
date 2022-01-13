#include "row_table.h"

#include <cstring>

namespace bytedance_db_project
{
  RowTable::RowTable() {}

  RowTable::~RowTable()
  {
    if (rows_ != nullptr)
    {
      delete rows_;
      rows_ = nullptr;
    }
  }

  void RowTable::Load(BaseDataLoader *loader)
  {
    num_cols_ = loader->GetNumCols();
    auto rows = loader->GetRows();
    num_rows_ = rows.size();
    rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
    for (auto row_id = 0; row_id < num_rows_; row_id++)
    {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_);
    }
  }

  int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id)
  {
    // TODO: Implement this!
    int32_t result;
    int32_t start =
        row_id * num_cols_ * FIXED_FIELD_LEN + col_id * FIXED_FIELD_LEN;
    result = rows_[start] + (rows_[start + 1] << 8) + (rows_[start + 2] << 16) +
             (rows_[start + 3] << 24);
    return result;
  }

  void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field)
  {
    // TODO: Implement this!
    char *temp = new char[4];
    temp[0] = (field & 0XFF);
    temp[1] = (field & 0XFF00);
    temp[2] = (field & 0XFF0000);
    temp[3] = (field & 0XFF000000);
    memcpy(rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN, temp,
           FIXED_FIELD_LEN);
  }

  int64_t RowTable::ColumnSum()
  {
    // TODO: Implement this!
    int64_t result = 0;
    for (int32_t i = 0; i < num_rows_; i++)
    {
      // char *temp = new char[FIXED_FIELD_LEN];
      // memcpy(temp, rows_ + i * FIXED_FIELD_LEN * num_cols_, FIXED_FIELD_LEN);
      // result += temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
      result += GetIntField(i, 0);
    }

    return result;
  }

  int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2)
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

  int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold)
  {
    // TODO: Implement this!
    int64_t result = 0;
    for (int32_t i = 0; i < num_rows_; i++)
    {
      if (GetIntField(i, 0) > threshold)
      {
        for (int32_t j = 0; j < num_cols_; j++)
        {
          result += GetIntField(i, j);
        }
      }
    }
    return result;
  }

  int64_t RowTable::PredicatedUpdate(int32_t threshold)
  {
    // TODO: Implement this!
    int64_t count = 0;
    for (int32_t i = 0; i < num_rows_; i++)
    {
      if (GetIntField(i, 0) < threshold)
      {
        int32_t result = GetIntField(i, 2) + GetIntField(i, 3);
        PutIntField(i, 3, result);
        count++;
      }
    }
    return count;
  }
} // namespace bytedance_db_project