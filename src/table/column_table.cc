#include "column_table.h"

#include <cstring>
#include <iostream>

namespace bytedance_db_project {
ColumnTable::ColumnTable() {}

ColumnTable::~ColumnTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
}

//
// columnTable, which stores data in column-major format.
// That is, data is laid out like
//   col 1 | col 2 | ... | col m.
//
void ColumnTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  std::vector<char *> rows = loader->GetRows();
  num_rows_ = rows.size();
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];

  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
}

int32_t ColumnTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  int32_t result;
  char *temp = new char[FIXED_FIELD_LEN];
  memcpy(temp,
         columns_ + col_id * num_rows_ * FIXED_FIELD_LEN +
             row_id * FIXED_FIELD_LEN,
         FIXED_FIELD_LEN);
  result = temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
  free(temp);
  return result;
}

void ColumnTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  char *temp = new char[4];
  temp[0] = (field & 0XFF);
  temp[1] = (field & 0XFF00);
  temp[2] = (field & 0XFF0000);
  temp[3] = (field & 0XFF000000);
  memcpy(columns_ + (col_id * num_rows_ + row_id) * FIXED_FIELD_LEN, temp,
         FIXED_FIELD_LEN);
  free(temp);
}

int64_t ColumnTable::ColumnSum() {
  // TODO: Implement this!
  int64_t result = 0;
  char *temp = new char[FIXED_FIELD_LEN];
  for (int32_t i = 0; i < num_rows_; i++) {
    memcpy(temp, columns_ + i * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
    result += temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
  }
  free(temp);
  return result;
}

int64_t ColumnTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t result = 0;
  for (int32_t i = 0; i < num_rows_; i++) {
    if (GetIntField(i, 1) > threshold1 && GetIntField(i, 2) < threshold2) {
      result += GetIntField(i, 0);
    }
  }
  return result;
  return 0;
}

int64_t ColumnTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t result = 0;
  for (int32_t i = 0; i < num_rows_; i++) {
    if (GetIntField(i, 0) > threshold) {
      for (int32_t j = 0; j < num_cols_; j++) {
        result += GetIntField(i, j);
      }
    }
  }
  return result;
  return 0;
}

int64_t ColumnTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t count = 0;
  for (int32_t i = 0; i < num_rows_; i++) {
    if (GetIntField(i, 0) < threshold) {
      int32_t result = GetIntField(i, 2) + GetIntField(i, 3);
      PutIntField(i, 3, result);
      count++;
    }
  }
  return count;
}
}  // namespace bytedance_db_project