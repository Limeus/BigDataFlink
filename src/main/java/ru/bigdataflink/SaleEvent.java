package ru.bigdataflink;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.Locale;

public class SaleEvent implements Serializable {
    private static final DateTimeFormatter CSV_DATE_FORMAT =
            DateTimeFormatter.ofPattern("M/d/yyyy", Locale.US);

    public String event_id;
    public String source_file;
    public String source_record_number;

    public String id;
    public String customer_first_name;
    public String customer_last_name;
    public String customer_age;
    public String customer_email;
    public String customer_country;
    public String customer_postal_code;
    public String customer_pet_type;
    public String customer_pet_name;
    public String customer_pet_breed;
    public String seller_first_name;
    public String seller_last_name;
    public String seller_email;
    public String seller_country;
    public String seller_postal_code;
    public String product_name;
    public String product_category;
    public String product_price;
    public String product_quantity;
    public String sale_date;
    public String sale_customer_id;
    public String sale_seller_id;
    public String sale_product_id;
    public String sale_quantity;
    public String sale_total_price;
    public String store_name;
    public String store_location;
    public String store_city;
    public String store_state;
    public String store_country;
    public String store_phone;
    public String store_email;
    public String pet_category;
    public String product_weight;
    public String product_color;
    public String product_size;
    public String product_brand;
    public String product_material;
    public String product_description;
    public String product_rating;
    public String product_reviews;
    public String product_release_date;
    public String product_expiry_date;
    public String supplier_name;
    public String supplier_contact;
    public String supplier_email;
    public String supplier_phone;
    public String supplier_address;
    public String supplier_city;
    public String supplier_country;

    public String eventId() {
        String value = blankToNull(event_id);
        if (value != null) {
            return value;
        }
        return sourcePrefix() + "#row-" + valueOrUnknown(id);
    }

    public String customerKey() {
        return sourcePrefix() + ":customer:" + valueOrUnknown(sale_customer_id);
    }

    public String sellerKey() {
        return sourcePrefix() + ":seller:" + valueOrUnknown(sale_seller_id);
    }

    public String productKey() {
        return sourcePrefix() + ":product:" + valueOrUnknown(sale_product_id);
    }

    public String supplierKey() {
        return "supplier:" + sha256(join(
                supplier_name,
                supplier_contact,
                supplier_email,
                supplier_phone,
                supplier_address,
                supplier_city,
                supplier_country
        )).substring(0, 32);
    }

    public String storeKey() {
        return "store:" + sha256(join(
                store_name,
                store_location,
                store_city,
                store_state,
                store_country,
                store_phone,
                store_email
        )).substring(0, 32);
    }

    public Integer sourceSaleId() {
        return intValue(id);
    }

    public Long sourceRecordNumber() {
        return longValue(source_record_number);
    }

    public Integer sourceCustomerId() {
        return intValue(sale_customer_id);
    }

    public Integer sourceSellerId() {
        return intValue(sale_seller_id);
    }

    public Integer sourceProductId() {
        return intValue(sale_product_id);
    }

    public Integer customerAge() {
        return intValue(customer_age);
    }

    public Integer productQuantity() {
        return intValue(product_quantity);
    }

    public Integer saleQuantity() {
        return intValue(sale_quantity);
    }

    public Integer productReviews() {
        return intValue(product_reviews);
    }

    public BigDecimal productPrice() {
        return decimalValue(product_price);
    }

    public BigDecimal saleTotalPrice() {
        return decimalValue(sale_total_price);
    }

    public BigDecimal productWeight() {
        return decimalValue(product_weight);
    }

    public BigDecimal productRating() {
        return decimalValue(product_rating);
    }

    public LocalDate saleDate() {
        return dateValue(sale_date);
    }

    public LocalDate productReleaseDate() {
        return dateValue(product_release_date);
    }

    public LocalDate productExpiryDate() {
        return dateValue(product_expiry_date);
    }

    public String normalizedSourceFile() {
        return valueOrUnknown(source_file);
    }

    public static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static Integer intValue(String value) {
        String normalized = blankToNull(value);
        return normalized == null ? null : Integer.valueOf(normalized);
    }

    public static Long longValue(String value) {
        String normalized = blankToNull(value);
        return normalized == null ? null : Long.valueOf(normalized);
    }

    public static BigDecimal decimalValue(String value) {
        String normalized = blankToNull(value);
        return normalized == null ? null : new BigDecimal(normalized);
    }

    public static LocalDate dateValue(String value) {
        String normalized = blankToNull(value);
        if (normalized == null) {
            return null;
        }
        try {
            return LocalDate.parse(normalized, CSV_DATE_FORMAT);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("Invalid date value: " + value, exception);
        }
    }

    private String sourcePrefix() {
        return normalizedSourceFile();
    }

    private static String valueOrUnknown(String value) {
        String normalized = blankToNull(value);
        return normalized == null ? "unknown" : normalized;
    }

    private static String join(String... values) {
        StringBuilder builder = new StringBuilder();
        for (String value : values) {
            builder.append(valueOrUnknown(value)).append('|');
        }
        return builder.toString();
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
