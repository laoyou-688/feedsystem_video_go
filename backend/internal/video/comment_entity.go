package video

import "time"

type Comment struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Username  string    `gorm:"index" json:"username"`
	VideoID   uint      `gorm:"index" json:"video_id"`
	AuthorID  uint      `gorm:"index" json:"author_id"`
	ClientToken string  `gorm:"size:64;index" json:"-"`
	Content   string    `gorm:"type:text" json:"content"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

type PublishCommentRequest struct {
	VideoID     uint   `json:"video_id"`
	Content     string `json:"content"`
	ClientToken string `json:"client_token"`
}

type DeleteCommentRequest struct {
	CommentID   uint   `json:"comment_id"`
	ClientToken string `json:"client_token"`
}

type GetAllCommentsRequest struct {
	VideoID uint `json:"video_id"`
}
