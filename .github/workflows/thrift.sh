#!/bin/bash

          changed_files=$(git diff --name-only [origin/master] [origin/check-thrift-local] | grep '\.thrift$')
          # 如果没有Thrift文件更改，则退出脚本
          if [[ -z "$changed_files" ]]
          then
          echo "No Thrift files have been changed"
          exit 0
          fi
  
          # 循环检查每个更改的Thrift文件
          for file in $changed_files
          do
          # 提取文件名和文件路径
          file_name=$(basename $file)
          file_path=$(dirname $file)
          echo "Checking Thrift file $file_name"
          echo "Checking Thrift file $file_path"
          
          # 判断Thrift是新增字段还是新增结构
          if grep -qP "struct $file_name\b" $file
          then
          echo "Thrift新增结构 $file_name"
          else
          echo "Thrift新增字段"
          # 提取新增字段名
          new_field=$(diff <(grep -oP 'struct \K\w+' $file_path/$file_name) <(grep -oP 'struct \K\w+' $file) | grep '>' | grep -oP '\.\K\w+')
          # 检查新增字段是否为required
          echo "Checking if new field $new_field is required"
          is_required=$(grep -oP "required \K$new_field" $file)
          if [[ "$is_required" == "required" ]]
          then
          echo "新增字段 $new_field 为required，出现错误"
          exit 1
          fi
          fi
          done