#include "mprpcconfig.h"

#include <iostream>
#include <string>

// 负责解析加载配置文件
void MprpcConfig::LoadConfigFile(const char *config_file) {
  FILE *pf = fopen(config_file, "r");
  if (nullptr == pf) {
    std::cout << config_file << " is note exist!" << std::endl;
    exit(EXIT_FAILURE);
  }

  // 1.注释   2.正确的配置项 =    3.去掉开头的多余的空格
  while (!feof(pf)) {
    char buf[512] = {0};
    fgets(buf, 512, pf);

    // 去掉字符串前面多余的空格
    std::string read_buf(buf);
    Trim(read_buf);

    // 判断#的注释
    if (read_buf[0] == '#' || read_buf.empty()) {
      continue;
    }

    // 解析配置项
    int idx = read_buf.find('=');
    if (idx == -1) {
      // 配置项不合法
      continue;
    }

    std::string key;
    std::string value;
    key = read_buf.substr(0, idx);
    Trim(key);
    // rpcserverip=127.0.0.1\n
    int endidx = read_buf.find('\n', idx);
    value = read_buf.substr(idx + 1, endidx - idx - 1);
    Trim(value);
    // 用赋值而非 insert：insert 在 key 已存在时【不覆盖】，即"首个写入的值生效"。
    // 而 RpcProvider::Run 是往配置里【追加】本节点 ip/port 的，一旦文件里出现重复 key
    // （手工编辑、绕过启动器的写入等），陈旧值会静默胜出 —— 表现为"节点连不上对端"，
    // 且不会有任何报错。改成后者覆盖，并对重复项显式告警。
    auto it = m_configMap.find(key);
    if (it != m_configMap.end()) {
      std::cout << "[MprpcConfig] 警告：配置项 \"" << key << "\" 重复出现，"
                << "旧值=\"" << it->second << "\" 被新值=\"" << value << "\" 覆盖（" << config_file << "）"
                << std::endl;
    }
    m_configMap[key] = value;
  }

  fclose(pf);
}

// 查询配置项信息
std::string MprpcConfig::Load(const std::string &key) {
  auto it = m_configMap.find(key);
  if (it == m_configMap.end()) {
    return "";
  }
  return it->second;
}

// 去掉字符串前后的空格
void MprpcConfig::Trim(std::string &src_buf) {
  int idx = src_buf.find_first_not_of(' ');
  if (idx != -1) {
    // 说明字符串前面有空格
    src_buf = src_buf.substr(idx, src_buf.size() - idx);
  }
  // 去掉字符串后面多余的空格
  idx = src_buf.find_last_not_of(' ');
  if (idx != -1) {
    // 说明字符串后面有空格
    src_buf = src_buf.substr(0, idx + 1);
  }
}