package repository

import (
	"context"
	"fmt"
	"notification-service/src/internal/entity"
	"notification-service/src/pkg/databases/mysql"
)

type NotificationRepository struct {
	DB mysql.DBInterface
}

func NewNotificationRepository(db mysql.DBInterface) *NotificationRepository {
	return &NotificationRepository{DB: db}
}

func (r *NotificationRepository) GetInboxNotifications(ctx context.Context, userID string, limit, offset int) ([]entity.Notification, error) {
	db, err := r.DB.GetDB()
	if err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT 
			id,
			notification_id,
			user_id,
			title,
			message,
			type,
			order_id,
			is_read,
			priority,
			metadata,
			created_at,
			read_at
		FROM notifications
		WHERE user_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`

	var notifications []entity.Notification
	if err := db.SelectContext(ctx, &notifications, query, userID, limit, offset); err != nil {
		return nil, err
	}

	return notifications, nil
}

func (r *NotificationRepository) MarkAsRead(ctx context.Context, notificationID, userID string) error {
	db, err := r.DB.GetDB()
	if err != nil {
		return err
	}

	query := `
		UPDATE notifications
		SET is_read = 1,
			read_at = NOW()
		WHERE notification_id = ? AND user_id = ?
	`

	result, err := db.ExecContext(ctx, query, notificationID, userID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		return fmt.Errorf("notification not found or already read")
	}

	return nil
}

func (r *NotificationRepository) SaveNotification(ctx context.Context, notif entity.Notification) error {
	db, err := r.DB.GetDB()
	if err != nil {
		return err
	}

	query := `
		INSERT INTO notifications (
			notification_id,
			user_id,
			title,
			message,
			type,
			order_id,
			is_read,
			priority,
			metadata,
			created_at,
			read_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	// pastikan pointer field aman
	var orderID any
	if notif.OrderID != nil {
		orderID = *notif.OrderID
	} else {
		orderID = nil
	}

	var readAt any
	if notif.ReadAt != nil {
		readAt = *notif.ReadAt
	} else {
		readAt = nil
	}

	_, err = db.ExecContext(
		ctx,
		query,
		notif.NotificationID,
		notif.UserID,
		notif.Title,
		notif.Message,
		notif.Type,
		orderID,
		notif.IsRead,
		notif.Priority,
		notif.Metadata,
		notif.CreatedAt,
		readAt,
	)
	if err != nil {
		return err
	}

	return nil
}
