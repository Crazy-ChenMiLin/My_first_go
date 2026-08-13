// Package web 把前端构建产物嵌进二进制。
//
// 整个服务最终只有一个 exe，没有 nginx、没有 dist 目录、没有路径配置。
// 拷贝走就能跑，这是 Go 做全栈最舒服的地方。
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// Assets 返回以 dist 为根的文件系统。
// 若前端尚未构建（只有占位 index.html），依然可以正常启动，
// 后端接口不受影响 —— 开发时前端跑 vite dev server 就够了。
func Assets() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil
	}
	return sub
}
