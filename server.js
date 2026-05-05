/**
 * 聊天室服务器 v6.0
 * Express + Socket.IO 实时聊天服务
 * 功能：注册登录、私聊/群聊、好友系统、拉黑、文件传输、后台管理、大厅系统、隐藏用户
 */

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

// ==================== 应用配置 ====================
const app = express();
const PORT = process.env.PORT || 3001;
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'admin123';
const TOKEN_EXPIRY_DAYS = 30; // Token 有效期（天）

// JSON 解析中间件（支持大文件上传）
app.use(express.json({ limit: '100mb' }));

// CORS 中间件 — 确保所有 API 路由在跨域场景下可用
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Admin-Key, Authorization');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// 创建 HTTP + Socket.IO 服务
const server = http.createServer(app);
const io = socketIo(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 5000,
  pingTimeout: 12000,
  transports: ['websocket', 'polling'],
  maxPayload: 100 * 1024 * 1024   // ★ 允许最大100MB（支持大文件base64上传）
});

// 静态文件服务（禁用缓存，防止 Cloudflare CDN 缓存旧版 JS）
app.use(function(req, res, next) {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  next();
});
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// ==================== 引导页（根路由） ====================
// 访问 / 时显示漂亮入口页，点击按钮进入聊天室
app.get('/', function(req, res) {
  var landingPath = path.join(__dirname, 'landing.html');
  if (fs.existsSync(landingPath)) {
    res.sendFile(landingPath);
  } else {
    // 没有引导页则直接跳转到聊天页
    res.redirect('/index.html');
  }
});

// ==================== 数据目录初始化 ====================
const dataDir = path.join(__dirname, 'data');
const uploadDir = path.join(__dirname, 'uploads');

[dataDir, uploadDir].forEach(function(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const ACCOUNTS_FILE = path.join(dataDir, 'accounts.json');
const BLOCKED_FILE = path.join(dataDir, 'blocked.json');
const MESSAGES_FILE = path.join(dataDir, 'messages.json');   // ★ 消息持久化存储文件

// ==================== 持久化存储工具 ====================

function loadJsonFile(filePath, defaultVal) {
  try {
    if (fs.existsSync(filePath)) {
      return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    }
  } catch (err) {
    console.error('[存储] 读取失败:', filePath, err.message);
  }
  return defaultVal;
}

function saveJsonFile(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');
    return true;
  } catch (err) {
    console.error('[存储] 写入失败:', filePath, err.message);
    return false;
  }
}

// ==================== 内存数据存储 ====================

var registeredAccounts = new Map();   // accountId -> 账号对象
var blockedUsers = new Set();        // 被封禁的用户ID集合
var storedMessages = {};              // ★ { chatId: [msg, ...] } 全量存储，10天后自动清除旧消息
var MESSAGE_RETENTION_DAYS = 10;       // 消息保留天数
var autoReplyMap = new Map();           // userId -> 自动回复配置 { enabled, defaultReply, rules: [{keyword, reply}] }
var AUTO_REPLY_FILE = path.join(dataDir, 'autoreply.json');  // 自动回复持久化文件
var autoReplyCooldown = new Map();      // 防循环冷却 { sender_target -> timestamp }

// 系统设置（大厅头像等）
var serverSettings = { lobbyAvatar: '' };
var SETTINGS_FILE = path.join(dataDir, 'settings.json');

/** 加载已存储的消息（同时清理超过保留天数的旧消息） */
function loadStoredMessages() {
  storedMessages = loadJsonFile(MESSAGES_FILE, {});
  var beforeCount = 0;
  var afterCount = 0;
  for (var key in storedMessages) {
    beforeCount += storedMessages[key].length;
    storedMessages[key] = storedMessages[key].filter(function(msg) {
      if (!msg.timestamp) return false;
      var msgTime = new Date(msg.timestamp).getTime();
      var cutoff = Date.now() - MESSAGE_RETENTION_DAYS * 24 * 60 * 60 * 1000;
      return msgTime > cutoff;
    });
    // 清理空数组
    if (storedMessages[key].length === 0) delete storedMessages[key];
    else afterCount += storedMessages[key].length;
  }
  console.log('[消息存储] 已加载 ' + Object.keys(storedMessages).length + ' 个聊天, 共 ' + afterCount + ' 条记录' +
              (beforeCount > afterCount ? ' (清理了 ' + (beforeCount - afterCount) + ' 条过期消息)' : ''));

  // 如果有清理，立即保存
  if (beforeCount > afterCount) persistMessages();
}

/**
 * 定期清理过期消息（建议每6小时调用一次）
 */
function cleanupOldMessages() {
  var now = Date.now();
  var cutoff = now - MESSAGE_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  var totalRemoved = 0;
  var cleanedKeys = [];

  for (var chatId in storedMessages) {
    var originalLen = storedMessages[chatId].length;
    storedMessages[chatId] = storedMessages[chatId].filter(function(msg) {
      if (!msg.timestamp) return true; // 无时间戳的保留
      return new Date(msg.timestamp).getTime() > cutoff;
    });
    totalRemoved += originalLen - storedMessages[chatId].length;
    if (storedMessages[chatId].length === 0) cleanedKeys.push(chatId);
  }
  // 删除空聊天
  cleanedKeys.forEach(function(k) { delete storedMessages[k]; });

  if (totalRemoved > 0) {
    persistMessages();
    console.log('[消息清理] 已清除 ' + totalRemoved + ' 条超过 ' + MESSAGE_RETENTION_DAYS + ' 天的旧消息');
  }
}

/** 保存消息到文件 */
function persistMessages() {
  saveJsonFile(MESSAGES_FILE, storedMessages);
}

/**
 * 存储一条消息到指定聊天ID（全量存储，不限制数量，由定时清理过期消息）
 */
function storeChatMessage(chatId, msg) {
  if (!storedMessages[chatId]) storedMessages[chatId] = [];
  var cleanMsg = {
    id: msg.id, type: msg.type,
    fromUserId: msg.fromUserId || null, fromUsername: msg.fromUsername || '', fromAvatar: msg.fromAvatar || '',
    content: msg.content || '', file: msg.file || null,
    timestamp: msg.timestamp || new Date().toISOString()
  };
  if (msg.to) cleanMsg.to = msg.to;
  if (msg.toUserId) cleanMsg.toUserId = msg.toUserId;
  if (msg.toUsername) cleanMsg.toUsername = msg.toUsername;
  if (msg.groupId) cleanMsg.groupId = msg.groupId;
  if (msg.groupName) cleanMsg.groupName = msg.groupName;
  if (msg.forwardedFromName) cleanMsg.forwardedFromName = msg.forwardedFromName;
  storedMessages[chatId].push(cleanMsg);
  persistMessages();
}

function initializeStorage() {
  var accounts = loadJsonFile(ACCOUNTS_FILE, []);
  accounts.forEach(function(acc) { registeredAccounts.set(acc.id, acc); });

  var blocked = loadJsonFile(BLOCKED_FILE, []);
  blocked.forEach(function(id) { blockedUsers.add(id); });

  // 加载持久化的聊天记录
  loadStoredMessages();

  // 加载自动回复配置
  var savedAutoReply = loadJsonFile(AUTO_REPLY_FILE, {});
  for (var uid in savedAutoReply) {
    autoReplyMap.set(uid, savedAutoReply[uid]);
  }

  // 加载系统设置
  var savedSettings = loadJsonFile(SETTINGS_FILE, {});
  if (savedSettings) {
    if (savedSettings.lobbyAvatar !== undefined) serverSettings.lobbyAvatar = savedSettings.lobbyAvatar;
  }

  console.log('[存储] 已加载 ' + registeredAccounts.size + ' 个账号, ' +
              blockedUsers.size + ' 个被拉黑用户, ' + autoReplyMap.size + ' 个自动回复配置');
}
initializeStorage();

function persistAccounts() {
  saveJsonFile(ACCOUNTS_FILE, Array.from(registeredAccounts.values()));
}

function persistBlocked() {
  saveJsonFile(BLOCKED_FILE, Array.from(blockedUsers));
}

/** 保存自动回复配置到文件 */
function persistAutoReply() {
  var obj = {};
  autoReplyMap.forEach(function(val, key) { obj[key] = val; });
  saveJsonFile(AUTO_REPLY_FILE, obj);
}

/** 保存系统设置到文件 */
function persistSettings() {
  saveJsonFile(SETTINGS_FILE, serverSettings);
}

/** 检查账号 token 是否已过期 */
function isTokenExpired(acc) {
  if (!acc.loginToken || !acc.tokenCreatedAt) return false; // 无创建时间则不检查
  var created = new Date(acc.tokenCreatedAt).getTime();
  var now = Date.now();
  return (now - created) > TOKEN_EXPIRY_DAYS * 24 * 60 * 60 * 1000;
}

/** 刷新账号的 token 和过期时间 */
function refreshToken(acc) {
  acc.loginToken = uuidv4() + '-' + Date.now();
  acc.tokenCreatedAt = new Date().toISOString();
  return acc.loginToken;
}

// ==================== 在线状态 & 会话数据 ====================

var usersMap = new Map();            // socketId -> 用户会话
var onlineUsers = new Map();         // socketId -> 在线用户信息
var groupsMap = new Map();           // groupId -> 群聊对象
var userRemarksMap = new Map();      // targetId -> 备注文字 (String)
var groupRemarksMap = new Map();     // groupId -> 备注文字 (String)
var friendRelations = new Map();     // userId -> Set(好友userId)
var pendingInvites = new Map();      // groupId -> [邀请记录数组]
var lobbyMessages = [];               // 大厅消息数组（实时用）
var hiddenUsersMap = new Map();        // userId -> Set(隐藏的用户Id)

// ==================== API 路由定义 ====================

// 服务器基本信息
app.get('/api/info', function(req, res) {
  res.json({
    status: 'ok',
    totalUsers: onlineUsers.size,
    registeredUsers: registeredAccounts.size,
    totalGroups: groupsMap.size
  });
});

// 获取所有已注册用户列表（用于客户端显示含离线用户的完整列表）
app.get('/api/users', function(req, res) {
  var list = [];
  registeredAccounts.forEach(function(acc) {
    if (acc.status === 'deleted') return;
    list.push({
      id: acc.id,
      username: acc.username,
      avatar: acc.avatar || 'default',
      registeredAt: acc.registeredAt
    });
  });
  res.json({ success: true, users: list });
});

// 用户注册（创建新账号）
app.post('/api/register', function(req, res) {
  var username = req.body.username;
  var avatar = req.body.avatar;

  if (!username || !username.trim()) {
    return res.json({ success: false, error: '请输入昵称' });
  }

  var cleanName = username.trim().substring(0, 20);

  // 检查重复用户名
  var nameExists = false;
  registeredAccounts.forEach(function(acc) {
    if (acc.username === cleanName && acc.status !== 'deleted') {
      nameExists = true;
    }
  });
  if (nameExists) {
    return res.json({ success: false, error: '该昵称已被使用' });
  }

  var accountId = uuidv4();
  var nowISO = new Date().toISOString();
  var account = {
    id: accountId,
    username: cleanName,
    avatar: avatar || 'default',
    registeredAt: nowISO,
    loginToken: uuidv4() + '-' + Date.now(),
    tokenCreatedAt: nowISO, // 记录 token 创建时间，用于过期检查
    status: 'active'
  };

  registeredAccounts.set(accountId, account);
  persistAccounts();

  console.log('[注册] "' + cleanName + '" (' + accountId.substring(0, 8) + '...)');

  res.json({
    success: true,
    account: {
      id: account.id,
      username: account.username,
      avatar: account.avatar,
      token: account.loginToken
    }
  });
});

// Token 自动登录验证
app.post('/api/login-token', function(req, res) {
  var token = req.body.token;

  var _loginIter = registeredAccounts.entries();
  var _loginEntry = _loginIter.next();
  while (!_loginEntry.done) {
    var accId = _loginEntry.value[0];
    var acc = _loginEntry.value[1];
    if (acc.loginToken === token && acc.status !== 'deleted') {
      // 检查是否被封禁
      if (blockedUsers.has(accId)) {
        return res.json({ success: false, error: '账号已被封禁' });
      }

      // 检查 token 是否过期
      if (isTokenExpired(acc)) {
        return res.json({ success: false, error: '登录已过期，请重新注册' });
      }

      // 刷新 token 有效期
      var newToken = refreshToken(acc);
      acc.lastLogin = new Date().toISOString();
      registeredAccounts.set(accId, acc);
      persistAccounts();

      return res.json({
        success: true,
        account: {
          id: acc.id,
          username: acc.username,
          avatar: acc.avatar,
          token: newToken
        }
      });
    }
    _loginEntry = _loginIter.next();
  }

  res.json({ success: false, error: '登录已过期' });
});

// 注销账号
app.post('/api/deactivate', function(req, res) {
  var token = req.body.token;
  var _deactIter = registeredAccounts.entries();
  var _deactEntry = _deactIter.next();
  while (!_deactEntry.done) {
    var accId = _deactEntry.value[0];
    var acc = _deactEntry.value[1];
    if (acc.loginToken === token) {
      acc.status = 'deleted';
      acc.deletedAt = new Date().toISOString();
      registeredAccounts.set(accId, acc);
      persistAccounts();

      // 强制该用户下线
      onlineUsers.forEach(function(u, sockId) {
        if (u.userId === accId) {
          io.to(sockId).emit('forceLogout', { reason: '账号已注销' });
        }
      });

      console.log('[注销] "' + acc.username + '"');
      return res.json({ success: true });
    }
    _deactEntry = _deactIter.next();
  }

  res.json({ success: false, error: '未找到账号' });
});

// ==================== 后台管理 API ====================

function requireAdminAuth(req, res, next) {
  var key = req.headers['x-admin-key'] || req.query.key;
  if (key !== ADMIN_SECRET) {
    return res.status(403).json({ success: false, error: '无管理员权限' });
  }
  next();
}

// 获取所有注册账号列表
app.get('/api/admin/accounts', requireAdminAuth, function(req, res) {
  var list = [];
  registeredAccounts.forEach(function(acc, id) {
    var isOnline = false;
    onlineUsers.forEach(function(u) {
      if (u.userId === id) isOnline = true;
    });

    list.push({
      id: acc.id,
      username: acc.username,
      avatar: acc.avatar,
      registeredAt: acc.registeredAt,
      lastLogin: acc.lastLogin || null,
      status: acc.status || 'active',
      isOnline: isOnline,
      isBlocked: blockedUsers.has(id)
    });
  });
  res.json({ success: true, accounts: list });
});

// 删除指定账号
app.delete('/api/admin/accounts/:id', requireAdminAuth, function(req, res) {
  var id = req.params.id;
  var acc = registeredAccounts.get(id);

  if (!acc) {
    return res.json({ success: false, error: '账号不存在' });
  }

  // 断开在线连接
  onlineUsers.forEach(function(u, sockId) {
    if (u.userId === id) {
      io.to(sockId).emit('forceLogout', { reason: '账号已被删除' });
    }
  });

  registeredAccounts.delete(id);
  blockedUsers.delete(id);
  persistAccounts();
  persistBlocked();

  console.log('[管理] 删除账号 "' + acc.username + '"');
  res.json({ success: true, message: '已删除账号: ' + acc.username });
});

// 封禁用户
app.post('/api/admin/block/:id', requireAdminAuth, function(req, res) {
  var id = req.params.id;

  if (!registeredAccounts.has(id)) {
    return res.json({ success: false, error: '账号不存在' });
  }

  blockedUsers.add(id);
  persistBlocked();

  // 断开并通知
  onlineUsers.forEach(function(u, sockId) {
    if (u.userId === id) {
      io.to(sockId).emit('forceLogout', { reason: '您已被封禁' });
    }
  });

  console.log('[管理] 封禁用户 ' + id);
  res.json({ success: true, message: '已拉黑该用户' });
});

// 解除封禁
app.post('/api/admin/unblock/:id', requireAdminAuth, function(req, res) {
  var id = req.params.id;
  blockedUsers.delete(id);
  persistBlocked();
  console.log('[管理] 解封用户 ' + id);
  res.json({ success: true, message: '已解除拉黑' });
});

// 在线用户详情
app.get('/api/admin/online', requireAdminAuth, function(req, res) {
  var list = [];
  onlineUsers.forEach(function(u) {
    list.push({
      socketId: u.socketId,
      userId: u.userId,
      username: u.username,
      avatar: u.avatar,
      connectedAt: u.connectedAt || null
    });
  });
  res.json({ success: true, online: list });
});

// 群聊列表（含成员详情）
app.get('/api/admin/groups', requireAdminAuth, function(req, res) {
  var list = [];
  groupsMap.forEach(function(g) {
    var memberList = [];
    if (g.members) {
      g.members.forEach(function(mid) {
        var found = null;
        onlineUsers.forEach(function(u) {
          if (u.socketId === mid) found = u;
        });
        memberList.push({
          memberId: mid,
          online: !!found,
          name: found ? found.username : '(离线)'
        });
      });
    }

    list.push({
      id: g.id,
      name: g.name,
      avatar: g.avatar,
      creator: g.creator,
      inviteOnly: g.inviteOnly,
      memberCount: g.members ? g.members.length : 0,
      members: memberList,
      createdAt: g.createdAt
    });
  });
  res.json({ success: true, groups: list });
});

// 删除群聊
app.delete('/api/admin/groups/:id', requireAdminAuth, function(req, res) {
  var id = req.params.id;
  var group = groupsMap.get(id);

  if (!group) {
    return res.json({ success: false, error: '群聊不存在' });
  }

  // 通知所有群成员
  if (group.members) {
    group.members.forEach(function(mid) {
      onlineUsers.forEach(function(u, sockId) {
        if (u.socketId === mid) {
          io.to(sockId).emit('groupDeleted', {
            groupId: id,
            name: group.name
          });
        }
      });
    });
  }

  groupsMap.delete(id);
  groupRemarksMap.delete(id);
  pendingInvites.delete(id);

  console.log('[管理] 删除群聊 "' + group.name + '"');
  res.json({ success: true, message: '已删除群聊: ' + group.name });
});

// 获取系统设置（无需管理员权限，前端需要读取大厅头像）
app.get('/api/settings', function(req, res) {
  res.json({ success: true, settings: serverSettings });
});

// 更新系统设置（管理员权限）
app.post('/api/admin/settings', requireAdminAuth, function(req, res) {
  var body = req.body || {};

  if (body.lobbyAvatar !== undefined) {
    serverSettings.lobbyAvatar = body.lobbyAvatar;
  }

  persistSettings();
  console.log('[管理] 系统设置已更新');

  // 广播设置变更给所有客户端
  io.emit('serverSettingsUpdated', serverSettings);

  res.json({ success: true, message: '设置已保存', settings: serverSettings });
});

// ==================== HTTP 文件上传（分片支持） ====================

var busboy = null;
try {
  busboy = require('busboy');
} catch(e) {
  console.log('[上传] busboy 未安装，使用备用上传方案');
}

/**
 * 处理文件分片上传
 * 支持大文件（单分片最大 50MB）
 */
app.post('/api/upload/chunk', function(req, res) {
  var chunks = [];
  var fields = {};
  var totalChunks = 0;
  var receivedChunks = 0;
  var fileName = '';
  var fileMime = '';

  if (busboy) {
    // 使用 busboy 解析 multipart（内存高效，适合大文件）
    var bb = new busboy({ headers: req.headers, limits: { fileSize: 50 * 1024 * 1024 } });
    var fileSize = 0;

    bb.on('field', function(fieldname, val) {
      fields[fieldname] = val;
    });

    bb.on('file', function(fieldname, file, filename, encoding, mimetype) {
      var buffers = [];
      file.on('data', function(data) {
        buffers.push(data);
        fileSize += data.length;
      });
      file.on('end', function() {
        chunks.push(Buffer.concat(buffers));
      });
      file.on('limit', function() {
        res.status(413).json({ success: false, error: '分片过大（单分片最大50MB）' });
        req.destroy();
      });
    });

    bb.on('finish', function() {
      var idx = parseInt(fields.chunkIndex || '0');
      var total = parseInt(fields.totalChunks || '1');
      var origName = fields.filename || 'unknown';
      var mime = fields.mimeType || 'application/octet-stream';
      var fSize = parseInt(fields.fileSize || '0');
      var fileId = fields.fileId || uuidv4();

      if (chunks.length === 0) {
        return res.json({ success: false, error: '没有文件数据' });
      }

      var chunkBuffer = chunks[0];
      var chunkPath = path.join(uploadDir, 'chunk_' + fileId + '_' + idx);
      fs.writeFileSync(chunkPath, chunkBuffer);

      res.json({
        success: true,
        fileId: fileId,
        chunkIndex: idx,
        totalChunks: total,
        received: chunkBuffer.length
      });
    });

    req.pipe(bb);

  } else {
    // 备用方案：使用原始 data 事件（内存占用较高）
    var rawData = [];
    var rawFields = {};
    var rawBoundary = req.headers['content-type'] ? req.headers['content-type'].match(/boundary=(.+)/) : null;

    if (!rawBoundary) {
      return res.status(400).json({ success: false, error: '无效的请求' });
    }

    req.on('data', function(d) { rawData.push(d); });
    req.on('end', function() {
      var body = Buffer.concat(rawData).toString('binary');

      // 简单解析 multipart（从 body 中提取字段值）
      function extractField(name) {
        var regex = new RegExp('name="' + name + '"\\r\\n\\r\\n([\\s\\S]*?)\\r\\n--');
        var m = body.match(regex);
        return m ? m[1] : '';
      }

      var idx = parseInt(extractField('chunkIndex') || '0');
      var total = parseInt(extractField('totalChunks') || '1');
      var origName = extractField('filename');
      var mime = extractField('mimeType');
      var fileId = extractField('fileId') || uuidv4();

      // 提取文件二进制数据
      var fileStart = body.indexOf('\r\n\r\n', body.indexOf('name="file"')) + 4;
      var fileEnd = body.lastIndexOf('\r\n--');
      if (fileStart > 0 && fileEnd > fileStart) {
        var fileData = Buffer.from(body.substring(fileStart, fileEnd), 'binary');
        var chunkPath = path.join(uploadDir, 'chunk_' + fileId + '_' + idx);
        fs.writeFileSync(chunkPath, fileData);

        res.json({
          success: true,
          fileId: fileId,
          chunkIndex: idx,
          totalChunks: total,
          received: fileData.length
        });
      } else {
        res.json({ success: false, error: '解析文件失败' });
      }
    });
  }
});

/**
 * 合并分片文件
 */
app.post('/api/upload/merge', function(req, res) {
  var body = req.body || {};
  var fileId = body.fileId;
  var totalChunks = body.totalChunks;
  var filename = body.filename;
  var mimeType = body.mimeType || '';
  var totalSize = body.fileSize || 0;

  if (!fileId || !totalChunks) {
    return res.json({ success: false, error: '缺少参数' });
  }

  // 合并所有分片
  var finalPath = path.join(uploadDir, uuidv4() + '_' + filename);
  var writeStream = fs.createWriteStream(finalPath);
  var mergedSize = 0;

  for (var i = 0; i < totalChunks; i++) {
    var chunkPath = path.join(uploadDir, 'chunk_' + fileId + '_' + i);
    if (!fs.existsSync(chunkPath)) {
      // 清理已写入的文件
      try { writeStream.close(); fs.unlinkSync(finalPath); } catch(e) {}
      return res.json({ success: false, error: '分片 ' + i + ' 不存在' });
    }
    var chunkData = fs.readFileSync(chunkPath);
    writeStream.write(chunkData);
    mergedSize += chunkData.length;
    // 删除已合并的分片
    try { fs.unlinkSync(chunkPath); } catch(e) {}
  }

  writeStream.end(function() {
    res.json({
      success: true,
      url: '/uploads/' + path.basename(finalPath),
      originalName: filename,
      size: mergedSize,
      mimeType: mimeType
    });
    console.log('[上传] 文件合并完成: ' + filename + ' (' + formatSizeForLog(mergedSize) + ')');
  });
});

function formatSizeForLog(bytes) {
  if (bytes < 1024) return bytes + 'B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + 'KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + 'MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + 'GB';
}

// ==================== Socket.IO 连接处理 ====================

io.on('connection', function(socket) {
  console.log('[连接] 新客户端接入: ' + socket.id);

  // ---------- 加入聊天 ----------
  socket.on('join', function(userData) {
    // 封禁检查
    if (userData.userId && blockedUsers.has(userData.userId)) {
      socket.emit('forceLogout', { reason: '您已被封禁，请联系管理员' });
      socket.disconnect(true);
      return;
    }

    // Token 过期检查（已注册用户）
    if (userData.userId) {
      var acc = registeredAccounts.get(userData.userId);
      if (acc && acc.status !== 'deleted' && isTokenExpired(acc)) {
        socket.emit('forceLogout', { reason: '登录已过期，请重新注册' });
        socket.disconnect(true);
        return;
      }
    }

    // 创建用户会话
    var sessionUser = {
      socketId: socket.id,
      userId: userData.userId || null,
      username: userData.username || '匿名',
      avatar: userData.avatar || 'default',
      connectedAt: new Date().toISOString()
    };

    usersMap.set(socket.id, sessionUser);
    onlineUsers.set(socket.id, sessionUser);

    // 广播最新用户列表和群聊列表
    broadcastUserList();
    broadcastGroupList();

    // 发送所有已注册用户列表（含离线用户，供客户端显示完整联系人）
    var allRegUsers = [];
    registeredAccounts.forEach(function(acc) {
      if (acc.status !== 'deleted') {
        allRegUsers.push({
          id: acc.id,
          username: acc.username,
          avatar: acc.avatar || 'default'
        });
      }
    });
    socket.emit('registeredUsersList', allRegUsers);

    // 发送个人数据
    sendRemarksToSocket(socket);
    sendHiddenListToSocket(socket);
    sendFriendListToSocket(socket);
    sendBlockedListToSocket(socket);

    // 发送自动回复配置
    var myId = sessionUser.userId || socket.id;
    var arConfig = autoReplyMap.get(myId) || { enabled: false, defaultReply: '', rules: [] };
    socket.emit('autoReplyConfig', arConfig);

    // 发送系统设置（大厅头像等）
    socket.emit('serverSettings', serverSettings);

    // 发送大厅历史消息
    socket.emit('lobbyMessages', lobbyMessages);

    // ★ 发送持久化的聊天历史记录（每个聊天最多10条）
    socket.emit('chatHistory', storedMessages);

    // 发送欢迎消息
    socket.emit('message', {
      type: 'system',
      content: '欢迎，' + sessionUser.username + '！',
      timestamp: new Date().toISOString()
    });
    socket.broadcast.emit('message', {
      type: 'system',
      content: sessionUser.username + ' 进入了聊天室',
      timestamp: new Date().toISOString()
    });

    console.log('[加入] "' + sessionUser.username + '" (' + socket.id + ')');
  });

  // ---------- 大厅消息 ----------
  socket.on('lobbyMessage', function(data) {
    var sender = usersMap.get(socket.id);
    if (!sender) return;

    // 封禁检查
    if (sender.userId && blockedUsers.has(sender.userId)) return;

    // Token 过期检查
    if (sender.userId) {
      var acc = registeredAccounts.get(sender.userId);
      if (acc && isTokenExpired(acc)) {
        socket.emit('forceLogout', { reason: '登录已过期' });
        return;
      }
    }

    var msg = {
      id: uuidv4(),
      type: 'lobby',
      fromUserId: sender.userId || null,
      fromUsername: sender.username,
      fromAvatar: sender.avatar,
      content: data.content || '',
      timestamp: new Date().toISOString()
    };
    lobbyMessages.push(msg);
    if (lobbyMessages.length > 200) lobbyMessages.shift();
    io.emit('lobbyMessage', msg);
    // ★ 持久化存储大厅消息（最多10条）
    storeChatMessage('lobby', msg);
  });

  // ---------- 私聊消息 ----------
  socket.on('privateMessage', function(data) {
    var sender = usersMap.get(socket.id);
    if (!sender) return;

    // 发送者封禁检查
    if (sender.userId && blockedUsers.has(sender.userId)) {
      socket.emit('error', { message: '您的账号已被限制' });
      return;
    }

    // Token 过期检查
    if (sender.userId) {
      var acc = registeredAccounts.get(sender.userId);
      if (acc && isTokenExpired(acc)) {
        socket.emit('forceLogout', { reason: '登录已过期' });
        return;
      }
    }

    // 查找目标接收者
    var targetEntry = findOnlineUserByIdOrSocket(data.to);
    var targetSocketId, targetUser;

    // ★ 目标用户可能离线，仍允许发消息（消息会被持久化，对方上线后同步）
    if (targetEntry) {
      targetSocketId = targetEntry[0];
      targetUser = targetEntry[1];
    } else {
      // 目标离线 — 尝试从已注册账号获取基本信息
      var targetAcc = null;
      registeredAccounts.forEach(function(a) {
        if (a.id === data.to || a.id === data.toUserId) targetAcc = a;
      });
      if (!targetAcc) {
        // 找不到目标账号
        socket.emit('error', { message: '用户不存在' });
        return;
      }
      // 离线用户的基本信息
      targetUser = {
        userId: targetAcc.id,
        username: targetAcc.username,
        avatar: targetAcc.avatar || 'default'
      };
      targetSocketId = null;  // 离线标记
    }

    // ★ 拉黑检查：接收方是否拉黑了发送者？
    var blockKey = 'block_' + (targetUser.userId || targetSocketId);
    var blockedSet = userRemarksMap.get(blockKey);
    if (blockedSet && blockedSet instanceof Set) {
      var senderIdToCheck = sender.userId || sender.socketId;
      if (blockedSet.has(senderIdToCheck)) {
        socket.emit('error', { message: '消息发送失败：对方已将你拉黑' });
        return;
      }
    }

    // 填充 toUserId 用于客户端匹配
    data.toUserId = targetUser.userId || data.to || targetSocketId;

    // 构造消息对象
    var msg = buildMessageObject('private', sender, data);

    // 接收者收到消息（仅在线时推送）
    if (targetSocketId) {
      io.to(targetSocketId).emit('message', msg);
    }

    // 发送者也收到回显
    socket.emit('message', msg);

    // ★ 持久化存储私聊消息（最多10天，由定时清理）
    var pChatId = 'private_' + [sender.userId || sender.socketId, targetUser.userId || targetSocketId].sort().join('_');
    storeChatMessage(pChatId, msg);

    // 如果对方在线，发送通知
    if (targetSocketId) {
      emitNotification(targetSocketId, {
        type: 'private_message',
        from: sender.username,
        fromAvatar: sender.avatar,
        content: data.content ? data.content.substring(0, 50) : '[文件]',
        chatId: sender.socketId
      });
    } else {
      // 对方离线 — 给发送者一个提示
      socket.emit('message', {
        type: 'system',
        content: targetUser.username + ' 当前离线，消息将在对方上线后送达',
        timestamp: new Date().toISOString()
      });
    }

    // ★ 自动回复逻辑：仅目标在线时触发（离线用户无法自动回复）
    var targetUserId = targetUser.userId || targetSocketId;
    var replyConfig = autoReplyMap.get(targetUserId);
    if (replyConfig && replyConfig.enabled && targetSocketId) {
      // 防循环冷却：同一对对话60秒内只自动回复一次
      var cooldownKey = targetUserId + '_' + (sender.userId || sender.socketId);
      var lastReplyTime = autoReplyCooldown.get(cooldownKey) || 0;
      if (Date.now() - lastReplyTime > 60000) {
        autoReplyCooldown.set(cooldownKey, Date.now());

        // 匹配关键词规则
        var replyText = replyConfig.defaultReply || '';
        if (replyConfig.rules && replyConfig.rules.length > 0 && data.content) {
          var lowerContent = data.content.toLowerCase();
          for (var ri = 0; ri < replyConfig.rules.length; ri++) {
            if (lowerContent.indexOf(replyConfig.rules[ri].keyword.toLowerCase()) !== -1) {
              replyText = replyConfig.rules[ri].reply;
              break;
            }
          }
        }

        if (replyText) {
          // 延迟1秒发送自动回复（模拟真实感）
          setTimeout(function() {
            var autoMsg = buildMessageObject('private', targetUser, {
              to: sender.socketId,
              toUsername: sender.username,
              content: '[自动回复] ' + replyText
            });
            // 标记为自动回复消息
            autoMsg.isAutoReply = true;

            socket.emit('message', autoMsg);  // 发送者收到自动回复
            io.to(targetSocketId).emit('message', autoMsg);  // 接收者也看到

            // 存储自动回复消息到聊天记录
            storeChatMessage(pChatId, autoMsg);
          }, 1000);
        }
      }
    }
  });

  // ---------- 群聊消息 ----------
  socket.on('groupMessage', function(data) {
    var sender = usersMap.get(socket.id);
    var group = groupsMap.get(data.groupId);

    if (!group || !sender) return;
    if (!group.members || group.members.indexOf(sender.socketId) === -1) return;

    // 封禁检查
    if (sender.userId && blockedUsers.has(sender.userId)) return;

    // Token 过期检查
    if (sender.userId) {
      var acc = registeredAccounts.get(sender.userId);
      if (acc && isTokenExpired(acc)) {
        socket.emit('forceLogout', { reason: '登录已过期' });
        return;
      }
    }

    var msg = buildMessageObject('group', sender, data);

    // 群内广播（除了发送者自己）
    if (group.members) {
      group.members.forEach(function(memberSockId) {
        if (memberSockId === sender.socketId) return; // 自己不重复发

        var mEntry = findOnlineUserBySocketId(memberSockId);
        if (mEntry) {
          io.to(memberSockId).emit('message', msg);
          emitNotification(memberSockId, {
            type: 'group_message',
            from: sender.username,
            group: group.name,
            content: data.content ? data.content.substring(0, 50) : '[文件]',
            chatId: data.groupId
          });
        }
      });
    }

    // 发送者自己也要看到消息（用于显示在界面上）
    socket.emit('message', msg);

    // ★ 持久化存储群聊消息（最多10条）
    storeChatMessage(data.groupId, msg);
  });

  // ---------- 创建群聊 ----------
  socket.on('createGroup', function(data) {
    var creator = usersMap.get(socket.id);
    if (!creator) return;

    var groupId = uuidv4();
    var newGroup = {
      id: groupId,
      name: data.name || '未命名群聊',
      avatar: data.avatar || 'default-group',
      creator: creator.socketId,
      creatorUserId: creator.userId,
      inviteOnly: data.inviteOnly !== false, // 默认邀请制
      members: [creator.socketId],
      createdAt: new Date().toISOString()
    };

    groupsMap.set(groupId, newGroup);
    pendingInvites.set(groupId, []);

    broadcastGroupList();

    socket.emit('message', {
      type: 'system',
      content: '群聊"' + newGroup.name + '"已创建！（邀请制：需要邀请才能加入）',
      timestamp: new Date().toISOString()
    });

    console.log('[创建群聊] "' + newGroup.name + '" by ' + creator.username);
  });

  // ---------- 邀请加入群聊 ----------
  socket.on('inviteToGroup', function(data) {
    var inviter = usersMap.get(socket.id);
    var group = groupsMap.get(data.groupId);
    var target = usersMap.get(data.targetSocketId);

    if (!inviter || !group || !target) return;
    if (!group.members || group.members.indexOf(inviter.socketId) === -1) return;
    if (group.members.indexOf(target.socketId) !== -1) return;

    // 记录邀请
    var invites = pendingInvites.get(data.groupId) || [];
    invites.push({
      fromId: inviter.socketId,
      fromName: inviter.username,
      toId: target.socketId,
      toName: target.username,
      timestamp: new Date().toISOString(),
      status: 'pending'
    });
    pendingInvites.set(data.groupId, invites);

    // 通知被邀请人
    io.to(target.socketId).emit('groupInvite', {
      groupId: group.id,
      groupName: group.name,
      groupAvatar: group.avatar,
      inviterName: inviter.username,
      inviterAvatar: inviter.avatar
    });

    socket.emit('message', {
      type: 'system',
      content: '已邀请 ' + target.username + ' 加入 "' + group.name + '"',
      timestamp: new Date().toISOString()
    });

    console.log('[群邀请] ' + inviter.username + ' -> ' + target.username + ' -> "' + group.name + '"');
  });

  // ---------- 接受群邀请 ----------
  socket.on('acceptGroupInvite', function(data) {
    var user = usersMap.get(socket.id);
    var group = groupsMap.get(data.groupId);

    if (!user || !group) return;
    if (group.members && group.members.indexOf(user.socketId) !== -1) return;

    // 加入成员列表
    if (!group.members) group.members = [];
    group.members.push(user.socketId);
    groupsMap.set(data.groupId, group);

    // 清理待处理邀请
    cleanInviteForUser(data.groupId, socket.id);

    // 通知群里所有人
    notifyGroupMembers(group, user.username + ' 加入了"' + group.name + '"');
  });

  // ---------- 拒绝群邀请 ----------
  socket.on('declineGroupInvite', function(data) {
    cleanInviteForUser(data.groupId, socket.id);

    var group = groupsMap.get(data.groupId);
    var user = usersMap.get(socket.id);

    if (group && user && group.creator) {
      var creatorEntry = findOnlineUserBySocketId(group.creator);
      if (creatorEntry) {
        io.to(creatorEntry[0]).emit('message', {
          type: 'system',
          content: user.username + ' 拒绝了 "' + group.name + '" 的邀请',
          timestamp: new Date().toISOString()
        });
      }
    }
  });

  // ---------- 加入公开群聊 ----------
  socket.on('joinGroup', function(groupId) {
    var user = usersMap.get(socket.id);
    var group = groupsMap.get(groupId);

    if (!user || !group) return;
    if (group.members && group.members.indexOf(user.socketId) !== -1) return;

    // 邀请制检查
    if (group.inviteOnly) {
      var invites = pendingInvites.get(groupId) || [];
      var hasInvitation = invites.some(function(inv) {
        return inv.toId === socket.id && inv.status === 'pending';
      });
      if (!hasInvitation) {
        socket.emit('message', {
          type: 'system',
          content: '"' + group.name + '" 是邀请制群聊，等待群成员邀请后即可加入',
          timestamp: new Date().toISOString()
        });
        return;
      }
    }

    // 加入群组
    if (!group.members) group.members = [];
    group.members.push(user.socketId);
    groupsMap.set(groupId, group);
    cleanInviteForUser(groupId, socket.id);

    notifyGroupMembers(group, user.username + ' 加入了"' + group.name + '"');
  });

  // ---------- 退出群聊 ----------
  socket.on('leaveGroup', function(groupId) {
    var user = usersMap.get(socket.id);
    var group = groupsMap.get(groupId);

    if (!user || !group) return;

    // 从成员列表移除
    if (group.members) {
      group.members = group.members.filter(function(m) { return m !== user.socketId; });
    }

    if (!group.members || group.members.length === 0) {
      // 群空了才解散
      groupsMap.delete(groupId);
      groupRemarksMap.delete(groupId);
      pendingInvites.delete(groupId);
    } else {
      // 如果退出的是群主，转让给第一个剩余成员
      if (group.creator === user.socketId) {
        group.creator = group.members[0];
        var newOwner = findOnlineUserBySocketId(group.members[0]);
        var newOwnerName = newOwner ? newOwner[1].username : '未知用户';
        console.log('[群主转让] "' + group.name + '" 群主从 ' + user.username + ' 转让给 ' + newOwnerName);
      }
      groupsMap.set(groupId, group);
      // 通知剩余成员更新群信息
      group.members.forEach(function(mid) {
        var me = findOnlineUserBySocketId(mid);
        if (me) {
          io.to(me[0]).emit('groupUpdate', group);
          // 如果转让了群主，单独通知新群主
          if (group.creator === mid) {
            io.to(me[0]).emit('message', {
              type: 'system',
              content: '你已成为群聊"' + group.name + '"的新群主',
              timestamp: new Date().toISOString()
            });
          }
        }
      });
    }

    broadcastGroupList();
  });

  // ---------- 踢人出群（仅群主/创建者） ----------
  socket.on('kickFromGroup', function(data) {
    var kicker = usersMap.get(socket.id);
    var group = groupsMap.get(data.groupId);
    var targetSockId = data.targetSocketId;

    if (!kicker || !group || !targetSockId) return;
    if (!group.members || group.members.indexOf(targetSockId) === -1) return;

    // 只有群主可以踢人
    if (group.creator !== socket.id) {
      socket.emit('toast', { message: '只有群主才能踢人', type: 'error' });
      return;
    }

    // 不能踢自己
    if (targetSockId === socket.id) return;

    // 从成员列表移除
    group.members = group.members.filter(function(m) { return m !== targetSockId });

    if (group.members.length === 0) {
      groupsMap.delete(data.groupId);
      groupRemarksMap.delete(data.groupId);
      pendingInvites.delete(data.groupId);
    } else {
      groupsMap.set(data.groupId, group);
    }

    // 通知被踢的人
    var kickedEntry = findOnlineUserBySocketId(targetSockId);
    if (kickedEntry) {
      io.to(kickedEntry[0]).emit('kickedFromGroup', {
        groupId: group.id,
        groupName: group.name
      });
      io.to(kickedEntry[0]).emit('message', {
        type: 'system',
        content: '你已被移出群聊"' + group.name + '"',
        timestamp: new Date().toISOString()
      });
    }

    // 通知剩余成员
    var kickedName = kickedEntry ? kickedEntry[1].username : '未知用户';
    group.members.forEach(function(mid) {
      var me = findOnlineUserBySocketId(mid);
      if (me) {
        io.to(me[0]).emit('groupUpdate', group);
        io.to(me[0]).emit('message', {
          type: 'system',
          content: kickedName + ' 已被移出群聊',
          timestamp: new Date().toISOString()
        });
      }
    });

    broadcastGroupList();
    console.log('[踢人] ' + kicker.username + ' 将 ' + kickedName + ' 踢出 "' + group.name + '"');
  });

  // ---------- 更新用户资料（昵称/头像）----------
  socket.on('updateUser', function(data) {
    var user = usersMap.get(socket.id);
    if (!user) return;

    if (data.username) user.username = data.username.substring(0, 20);
    if (data.avatar) user.avatar = data.avatar;

    usersMap.set(socket.id, user);
    onlineUsers.set(socket.id, user);
    broadcastUserList();

    // 同步到持久化的账号数据
    if (user.userId) {
      var acc = registeredAccounts.get(user.userId);
      if (acc) {
        acc.username = user.username;
        acc.avatar = user.avatar;
        registeredAccounts.set(user.userId, acc);
        persistAccounts();
      }
    }
  });

  // ---------- 好友操作：添加好友 ----------
  socket.on('addFriend', function(data) {
    var me = usersMap.get(socket.id);
    var targetEntry = findOnlineUserByIdOrSocket(data.targetId);

    if (!me || !targetEntry) return;

    var myId = me.userId || me.socketId;
    var theirId = targetEntry[1].userId || targetEntry[1].socketId;

    initFriendSet(myId);
    initFriendSet(theirId);

    friendRelations.get(myId).add(theirId);
    friendRelations.get(theirId).add(myId);

    // 双方刷新好友列表
    sendFriendListToSocket(io.to(socket.id));
    sendFriendListToSocket(io.to(targetEntry[0]));

    socket.emit('message', {
      type: 'system',
      content: '已添加 ' + targetEntry[1].username + ' 为好友',
      timestamp: new Date().toISOString()
    });

    io.to(targetEntry[0]).emit('message', {
      type: 'system',
      content: me.username + ' 添加你为好友',
      timestamp: new Date().toISOString()
    });
  });

  // ---------- 好友操作：删除 / 拉黑 ----------
  socket.on('removeFriend', function(data) {
    var me = usersMap.get(socket.id);
    if (!me) return;

    var myId = me.userId || me.socketId;
    var targetId = data.targetId;

    if (!friendRelations.has(myId)) return;
    friendRelations.get(myId).delete(targetId);

    if (friendRelations.has(targetId)) {
      friendRelations.get(targetId).delete(myId);
    }

    // 如果是拉黑操作，记录拉黑关系
    if (data.block) {
      var blockKey = 'block_' + myId;
      if (!userRemarksMap.has(blockKey)) {
        userRemarksMap.set(blockKey, new Set());
      }
      userRemarksMap.get(blockKey).add(targetId);
      broadcastRemarksData();
    }

    sendFriendListToSocket(io.to(socket.id));

    socket.emit('message', {
      type: 'system',
      content: data.block ? '已拉黑该用户' : '已移除好友',
      timestamp: new Date().toISOString()
    });
  });

  // ---------- 解除拉黑 ----------
  socket.on('unblockUser', function(data) {
    var me = usersMap.get(socket.id);
    if (!me) return;

    var myId = me.userId || me.socketId;
    var blockKey = 'block_' + myId;

    if (userRemarksMap.has(blockKey)) {
      userRemarksMap.get(blockKey).delete(data.targetId);
      broadcastRemarksData();
    }

    // 恢复好友关系
    initFriendSet(myId);
    friendRelations.get(myId).add(data.targetId);

    if (friendRelations.has(data.targetId)) {
      friendRelations.get(data.targetId).add(myId);
    }

    sendFriendListToSocket(io.to(socket.id));
    showToastMsg(socket.id, '已恢复好友关系');
  });

  // ---------- 自动回复设置 ----------
  socket.on('setAutoReply', function(data) {
    var me = usersMap.get(socket.id);
    if (!me) return;
    var myId = me.userId || me.socketId;

    var config = {
      enabled: !!data.enabled,
      defaultReply: (data.defaultReply || '').substring(0, 500),
      rules: []
    };

    // 处理关键词规则
    if (data.rules && Array.isArray(data.rules)) {
      data.rules.forEach(function(rule) {
        if (rule.keyword && rule.reply) {
          config.rules.push({
            keyword: rule.keyword.substring(0, 100),
            reply: rule.reply.substring(0, 500)
          });
        }
      });
      // 最多10条规则
      config.rules = config.rules.slice(0, 10);
    }

    autoReplyMap.set(myId, config);
    persistAutoReply();

    socket.emit('autoReplyUpdated', { success: true, config: config });
    console.log('[自动回复] ' + me.username + ' 更新了自动回复配置');
  });

  // ---------- 获取自动回复配置 ----------
  socket.on('getAutoReply', function() {
    var me = usersMap.get(socket.id);
    if (!me) return;
    var myId = me.userId || me.socketId;
    var config = autoReplyMap.get(myId) || { enabled: false, defaultReply: '', rules: [] };
    socket.emit('autoReplyConfig', config);
  });

  // ---------- 保存备注 ----------
  socket.on('saveRemark', function(data) {
    if (data.type === 'user') {
      if (data.remark && data.remark.trim()) {
        userRemarksMap.set(data.targetId, data.remark.trim());
      } else {
        userRemarksMap.delete(data.targetId);
      }
    } else if (data.type === 'group') {
      if (data.remark && data.remark.trim()) {
        groupRemarksMap.set(data.targetId, data.remark.trim());
      } else {
        groupRemarksMap.delete(data.targetId);
      }
    }
    broadcastRemarksData();
  });

  // ---------- 隐藏用户 ----------
  socket.on('hideUser', function(data) {
    var me = usersMap.get(socket.id);
    if (!me) return;
    var myId = me.userId || me.socketId;
    if (!hiddenUsersMap.has(myId)) hiddenUsersMap.set(myId, new Set());
    hiddenUsersMap.get(myId).add(data.targetId);
    sendHiddenListToSocket(io.to(socket.id));
  });

  socket.on('unhideUser', function(data) {
    var me = usersMap.get(socket.id);
    if (!me) return;
    var myId = me.userId || me.socketId;
    if (hiddenUsersMap.has(myId)) hiddenUsersMap.get(myId).delete(data.targetId);
    sendHiddenListToSocket(io.to(socket.id));
  });

  // ---------- 图片上传（头像）----------
  socket.on('uploadAvatar', function(data, callback) {
    handleImageUpload(data, callback);
  });

  // ---------- 文件上传 ----------
  socket.on('uploadFile', function(data, callback) {
    handleFileUpload(data, callback);
  });

  // ---------- 已读标记（保留接口）----------
  socket.on('markRead', function() {
    // 可扩展为已读回执逻辑
  });

  // ---------- 断开连接 ----------
  socket.on('disconnect', function(reason) {
    var user = usersMap.get(socket.id);
    if (!user) return;

    console.log('[断开] ' + user.username + ' (' + socket.id + ') 原因: ' + reason);

    // 清理在线状态
    usersMap.delete(socket.id);
    onlineUsers.delete(socket.id);

    // 从所有群聊中移除
    groupsMap.forEach(function(g, gid) {
      if (g.members && g.members.indexOf(socket.id) !== -1) {
        g.members = g.members.filter(function(m) { return m !== socket.id; });

        if (g.members.length === 0) {
          // 群空了解散
          groupsMap.delete(gid);
          groupRemarksMap.delete(gid);
          pendingInvites.delete(gid);
        } else {
          // 通知剩余成员
          g.members.forEach(function(mid) {
            var me = findOnlineUserBySocketId(mid);
            if (me) {
              io.to(me[0]).emit('groupUpdate', g);
            }
          });
        }
      }
    });

    broadcastUserList();
    broadcastGroupList();

    socket.broadcast.emit('message', {
      type: 'system',
      content: user.username + ' 离开了聊天室',
      timestamp: new Date().toISOString()
    });
  });
});

// ==================== 辅助函数 ====================

/** 构建消息对象 */
function buildMessageObject(type, fromUser, data) {
  var msg = {
    id: uuidv4(),
    type: type,
    from: fromUser.socketId,
    fromUserId: fromUser.userId || null,
    fromUsername: fromUser.username,
    fromAvatar: fromUser.avatar,
    content: data.content || '',
    file: data.file || null,
    timestamp: new Date().toISOString()
  };

  if (type === 'private') {
    msg.to = data.to;
    msg.toUsername = data.toUsername;
    msg.toUserId = data.toUserId || null; // 新增：接收方userId，用于客户端精确匹配
  } else if (type === 'group') {
    msg.groupId = data.groupId;
    msg.groupName = data.groupName;
  }

  return msg;
}

/** 处理图片上传（base64 → 文件） */
function handleImageUpload(data, callback) {
  if (!data.base64Data) {
    return callback({ success: false, error: '无图片数据' });
  }

  var match = data.base64Data.match(/^data:(.+);base64,(.+)$/);
  if (!match) {
    return callback({ success: false, error: '图片格式错误' });
  }

  var ext = (match[1].split('/')[1]) || 'png';
  var filename = uuidv4() + '.' + ext;
  var filePath = path.join(uploadDir, filename);

  fs.writeFile(filePath, match[2], 'base64', function(err) {
    if (err) {
      return callback({ success: false, error: err.message });
    }
    callback({ success: true, url: '/uploads/' + filename });
  });
}

/** 处理通用文件上传 */
function handleFileUpload(data, callback) {
  if (!data.base64Data || !data.filename) {
    return callback({ success: false, error: '缺少必要参数' });
  }
  if (data.size > 100 * 1024 * 1024) {
    return callback({ success: false, error: '文件过大（>100MB）' });
  }

  var filePath = path.join(uploadDir, uuidv4() + '_' + data.filename);

  fs.writeFile(filePath, data.base64Data, 'base64', function(err) {
    if (err) {
      return callback({ success: false, error: err.message });
    }
    callback({
      success: true,
      url: '/uploads/' + path.basename(filePath),
      originalName: data.filename,
      size: data.size,
      mimeType: data.mimeType || ''
    });
  });
}

/** 通过 socketId 或 userId 查找在线用户 */
function findOnlineUserBySocketId(sockId) {
  var user = onlineUsers.get(sockId);
  if (user) return [sockId, user];
  return null;
}

/** 通过 ID（可能是 socketId 或 userId）查找在线用户 */
function findOnlineUserByIdOrSocket(id) {
  var result = null;
  var _iter = onlineUsers.entries();
  var _entry = _iter.next();
  while (!_entry.done) {
    var sid = _entry.value[0];
    var u = _entry.value[1];
    if (sid === id || u.userId === id) {
      result = [sid, u];
      break;  // 找到后立即退出
    }
    _entry = _iter.next();
  }
  return result;
}

/** 广播用户列表给所有人 */
function broadcastUserList() {
  var list = [];
  onlineUsers.forEach(function(u) { list.push(u); });
  io.emit('userList', list);
}

/** 广播群聊列表给所有人 */
function broadcastGroupList() {
  var list = [];
  groupsMap.forEach(function(g) { list.push(g); });
  io.emit('groupList', list);
}

/** 向单个 Socket 发送备注数据 */
function sendRemarksToSocket(sock) {
  var r = {};

  userRemarksMap.forEach(function(v, k) {
    if (typeof v === 'string') {
      r['user_' + k] = v;
    } else if (v instanceof Set) {
      r['block_' + k.substring(6)] = Array.from(v);
    }
  });

  groupRemarksMap.forEach(function(v, k) {
    r['group_' + k] = v;
  });

  sock.emit('remarksData', r);

  // 同时发送隐藏用户列表
  sendHiddenListToSocket(sock);
}

/** 向单个 Socket 发送隐藏用户列表（全局版本，供 join 等外部调用） */
function sendHiddenListToSocket(sock) {
  var list = {};
  hiddenUsersMap.forEach(function(set, uid) { list[uid] = Array.from(set); });
  sock.emit('hiddenListData', list);
}

/** 向单个 Socket 发送好友列表 */
function sendFriendListToSocket(sock) {
  var list = {};
  friendRelations.forEach(function(set, uid) {
    list[uid] = Array.from(set);
  });
  sock.emit('friendListData', list);
}

/** 向单个 Socket 发送拉黑列表 */
function sendBlockedListToSocket(sock) {
  var blocks = {};
  userRemarksMap.forEach(function(v, k) {
    if (k.startsWith('block_') && v instanceof Set) {
      blocks[k.substring(6)] = Array.from(v);
    }
  });
  sock.emit('blockedList', blocks);
}

/** 广播备注+好友数据给所有在线用户 */
function broadcastRemarksData() {
  var r = {};
  userRemarksMap.forEach(function(v, k) {
    var key = k.startsWith('block_') ? k : ('user_' + k);
    r[key] = typeof v === 'string' ? v :
           (v instanceof Set ? Array.from(v) : v);
  });

  groupRemarksMap.forEach(function(v, k) {
    r['group_' + k] = v;
  });

  io.emit('remarksData', r);

  // 同时广播好友和拉黑状态
  var fl = {};
  friendRelations.forEach(function(s, uid) { fl[uid] = Array.from(s); });
  io.emit('friendListData', fl);
}

/** 清理某用户的特定群邀请 */
function cleanInviteForUser(groupId, socketId) {
  var invites = pendingInvites.get(groupId) || [];
  invites = invites.filter(function(i) { return i.toId !== socketId; });
  pendingInvites.set(groupId, invites);
}

/** 向群成员广播系统消息 */
function notifyGroupMembers(group, sysText) {
  if (group.members) {
    group.members.forEach(function(mid) {
      var me = findOnlineUserBySocketId(mid);
      if (me) {
        io.to(me[0]).emit('groupUpdate', group);
        io.to(me[0]).emit('message', {
          type: 'system',
          content: sysText,
          timestamp: new Date().toISOString()
        });
      }
    });
  }
}

/** 初始化好友集合 */
function initFriendSet(userId) {
  if (!friendRelations.has(userId)) {
    friendRelations.set(userId, new Set());
  }
}

/** 发送通知 */
function emitNotification(socketId, notification) {
  io.to(socketId).emit('notification', notification);
}

/** 发送 Toast 提示 */
function showToastMsg(socketId, text) {
  io.to(socketId).emit('toast', { message: text, type: 'info' });
}

// ==================== 获取本机 IP ====================

function getLocalIP() {
  try {
    var interfaces = require('os').networkInterfaces();
    for (var name in interfaces) {
      for (var i = 0; i < interfaces[name].length; i++) {
        var iface = interfaces[name][i];
        if (iface.family === 'IPv4' && iface.address !== '127.0.0.1' && !iface.internal) {
          return iface.address;
        }
      }
    }
  } catch (e) {}
  return 'localhost';
}

// ==================== 启动监听 ====================

server.on('error', function(err) {
  if (err.code === 'EADDRINUSE') {
    console.error('');
    console.error('[错误] 端口 ' + PORT + ' 已被占用！请先关闭占用该端口的进程：');
    console.error('  方法1: 按 Ctrl+C 关闭之前的窗口');
    console.error('  方法2: 运行 netstat -ano | findstr :' + PORT + ' 找到 PID 后结束进程');
    console.error('');
  } else {
    throw err;
  }
});

server.listen(PORT, '0.0.0.0', function() {
  var ip = getLocalIP();
  console.log('');
  console.log('==========================================');
  console.log('   聊天服务器 v6.0 已启动！');
  console.log('==========================================');
  console.log('  本地：   http://localhost:' + PORT);
  console.log('  局域网：http://' + ip + ':' + PORT);
  console.log('  管理：   http://localhost:' + PORT + '/host.html');
  console.log('  管理密钥：' + ADMIN_SECRET);
  console.log('  消息保留：' + MESSAGE_RETENTION_DAYS + ' 天（自动清理过期记录）');
  console.log('==========================================');
  console.log('');

  // ★ 每6小时自动清理超过保留天数的旧消息
  setInterval(cleanupOldMessages, 6 * 60 * 60 * 1000);
});
