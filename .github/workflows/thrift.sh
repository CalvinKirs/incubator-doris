#!/bin/bash

# 检查所有更改的 Thrift 文件
for file in $(git diff --name-only HEAD^ HEAD | grep '\.thrift$'); do
  echo "Checking changes in $file"
  
  # 检查每个文件中的变更
  while read -r line; do
    # 判断变更类型
    if echo "$line" | grep -q '^\+.*\bstruct\b'; then
      # 新增结构
      struct=$(echo "$line" | sed 's/^\+//')
      echo "New struct: $struct"
    elif echo "$line" | grep -q '^\+.*\bfield\b'; then
      # 新增字段
      field=$(echo "$line" | sed 's/^\+//')
      echo "New field: $field"
      if echo "$field" | grep -q '\brequired\b'; then
        # 检查 required 字段
        echo "Error: Required field added: $field"
        exit 1
      fi
    elif echo "$line" | grep -q '^\-.*\benum\b'; then
      # 删除枚举
      echo "Error: Enum deleted: $line"
      exit 1
    fi
  done < <(git diff HEAD^ HEAD "$file" | grep '^\+' | sed 's/^\+//')
done