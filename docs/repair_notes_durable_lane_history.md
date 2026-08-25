# Durable lane history repair

The candidate-observatory historical replay certifies only the frozen pre-live window. That contract remains unchanged and fail-closed.

The dashboard now exposes a separate read-only durable-history summary spanning the configured replay start through the current read time. It reports trustworthy persisted source and operating history for every canonical lane without synthesizing candidate identities or treating historical observations as forward qualification evidence.

This separation prevents a lane that began collecting after the first-live observatory boundary from being displayed as having no history merely because it cannot satisfy the earlier pre-live certification window.
