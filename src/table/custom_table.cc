#include "custom_table.h"

#include <algorithm>
#include <cstring>
namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  char *temp = new char[FIXED_FIELD_LEN];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
    std::memcpy(temp, cur_row, FIXED_FIELD_LEN);
    int32_t index0data =
        temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
    index0_[index0data].push_back(row_id);

    std::memcpy(temp, cur_row + FIXED_FIELD_LEN, FIXED_FIELD_LEN);
    int32_t index1data =
        temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
    index1_[index1data].push_back(row_id);

    std::memcpy(temp, cur_row + 2 * FIXED_FIELD_LEN, FIXED_FIELD_LEN);
    int32_t index2data =
        temp[0] + (temp[1] << 8) + (temp[2] << 16) + (temp[3] << 24);
    index2_[index2data].push_back(row_id);
  }
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
  free(temp);
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
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

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  int32_t oldvalue = GetIntField(row_id, col_id);
  char *temp = new char[4];
  temp[0] = (field & 0XFF);
  temp[1] = (field & 0XFF00);
  temp[2] = (field & 0XFF0000);
  temp[3] = (field & 0XFF000000);
  memcpy(rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN, temp,
         FIXED_FIELD_LEN);
  memcpy(columns_ + (col_id * num_rows_ + row_id) * FIXED_FIELD_LEN, temp,
         FIXED_FIELD_LEN);
  if (col_id == 0) {
    auto it = index0_[oldvalue].begin();
    while (it != index0_[oldvalue].end() && (*it != row_id)) {
      it++;
    }
    if (it != index0_[oldvalue].end()) {
      index0_[oldvalue].erase(it);
      if (index0_[oldvalue].size() == 0) {
        index0_.erase(oldvalue);
      }
    }
    index0_[field].push_back(row_id);
  }

  if (col_id == 1) {
    auto it = index1_[oldvalue].begin();
    while (it != index1_[oldvalue].end() && (*it != row_id)) {
      it++;
    }
    if (it != index1_[oldvalue].end()) {
      index1_[oldvalue].erase(it);
      if (index1_[oldvalue].size() == 0) {
        index1_.erase(oldvalue);
      }
    }
    index1_[field].push_back(row_id);
  }

  if (col_id == 0) {
    auto it = index2_[oldvalue].begin();
    while (it != index2_[oldvalue].end() && (*it != row_id)) {
      it++;
    }
    if (it != index2_[oldvalue].end()) {
      index2_[oldvalue].erase(it);
      if (index2_[oldvalue].size() == 0) {
        index2_.erase(oldvalue);
      }
    }
    index2_[field].push_back(row_id);
  }
  free(temp);
}

int64_t CustomTable::ColumnSum() {
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

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t result = 0;
  std::vector<int32_t> row1;
  std::vector<int32_t> row2;
  std::vector<int32_t> interrow;
  for (auto it = index1_.begin(); it != index1_.end(); it++) {
    if (it->first > threshold1) {
      row1.insert(row1.end(), it->second.begin(), it->second.end());
    }
  }
  for (auto it = index2_.begin(); it != index2_.end(); it++) {
    if (it->first < threshold2) {
      row2.insert(row2.end(), it->second.begin(), it->second.end());
    }
  }
  std::sort(row1.begin(), row1.end());
  std::sort(row2.begin(), row2.end());
  std::set_intersection(row1.begin(), row1.end(), row2.begin(), row2.end(),
                        std::back_inserter(interrow));
  for (auto it = interrow.begin(); it != interrow.end(); it++) {
    result += GetIntField(*it, 0);
  }
  return result;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t result = 0;
  for (auto it = index0_.begin(); it != index0_.end(); it++) {
    if (it->first > threshold) {
      for (int32_t i = 0; i < (int32_t)it->second.size(); i++) {
        for (int32_t j = 0; j < num_cols_; j++) {
          result += GetIntField(it->second[i], j);
        }
      }
    }
  }
  return result;
}

int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t count = 0;
  for (auto it = index0_.begin(); it != index0_.end(); it++) {
    if (it->first < threshold) {
      for (int32_t i = 0; i < (int32_t)it->second.size(); i++) {
        int32_t reslut =
            GetIntField(it->second[i], 2) + GetIntField(it->second[i], 3);
        PutIntField(it->second[i], 3, reslut);
        count++;
      }
    }
  }
  return count;
}
}  // namespace bytedance_db_project