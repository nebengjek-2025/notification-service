package model

import "time"

type NotificationItem struct {
	NotificationID string     `json:"notification_id"`
	Title          string     `json:"title"`
	Message        string     `json:"message"`
	Type           string     `json:"type"`
	OrderID        *string    `json:"order_id,omitempty"`
	IsRead         bool       `json:"is_read"`
	Priority       string     `json:"priority"`
	CreatedAt      time.Time  `json:"created_at"`
	ReadAt         *time.Time `json:"read_at,omitempty"`
}

type InboxNotificationResponse struct {
	UserID        string             `json:"user_id"`
	Notifications []NotificationItem `json:"notifications"`
}

type OrderNotificationMessage struct {
	DriverID    string `json:"driverId"`
	PassengerID string `json:"passangerId"`
	OrderID     string `json:"orderId"`
}

type OrderNotificationEvent struct {
	ID      string                   `json:"id"`
	Message OrderNotificationMessage `json:"message"`
}

type ReadNotificationRequest struct {
	NotificationID string `json:"notification_id"`
	UserID         string `json:"user_id"`
}
