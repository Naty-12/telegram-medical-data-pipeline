-- medical_telegram_dwh/tests/assert_has_image_implies_images_attached.sql
SELECT
    message_id
FROM
    {{ ref('fct_messages') }}
WHERE
    has_image_attachment = TRUE
    AND num_images_attached = 0