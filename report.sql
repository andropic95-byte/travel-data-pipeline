SELECT booked_at AS booking_date, origin, destination, currency,
       COUNT(*) AS bookings,
       SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) AS confirmed,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN status = 'confirmed' THEN price_minor ELSE 0 END) / 100.0 AS confirmed_value,
       ROUND(AVG(CASE WHEN status = 'confirmed' THEN price_minor END) / 100.0, 2) AS average_confirmed_price
FROM bookings
GROUP BY booked_at, origin, destination, currency
ORDER BY booking_date, origin, destination, currency;
