# Frontend

该目录是短视频 Feed 系统的前端页面工程，基于 Vue3 + Vite 实现，主要用于联调后端接口、展示 Feed 流页面，并覆盖账号、视频、点赞、评论、关注等核心交互。

## 功能覆盖

- Account：注册、登录、改密、改名、登出、账号查询
- Video：视频发布、作者视频列表、视频详情
- Like：点赞、取消点赞、点赞状态查询
- Comment：评论发布、评论列表、删除评论
- Social：关注、取关、粉丝列表、关注列表
- Feed：最新流、热度流、关注流

## 本地启动

建议先启动后端 API：

```bash
cd backend
go run ./cmd
```

再启动前端：

```bash
cd frontend
npm install
npm run dev
```

## 请求转发

开发模式下，前端通过 Vite 代理将 `/api/*` 请求转发到本地后端服务。默认目标地址可在 `frontend/vite.config.ts` 中查看和调整。

## 说明

- 该前端主要用于接口联调和功能验证
- 如需对接其他后端地址，可修改 Vite 代理配置
