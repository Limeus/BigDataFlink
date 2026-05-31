package ru.bigdataflink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

public class StarSchemaJdbcSink extends RichSinkFunction<SaleEvent> {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement upsertCustomer;
    private transient PreparedStatement upsertSeller;
    private transient PreparedStatement upsertSupplier;
    private transient PreparedStatement upsertProduct;
    private transient PreparedStatement upsertStore;
    private transient PreparedStatement upsertDate;
    private transient PreparedStatement upsertFact;

    public StarSchemaJdbcSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);

        upsertCustomer = connection.prepareStatement("""
                INSERT INTO star.dim_customer (
                    customer_key, source_file, source_customer_id, first_name, last_name, age,
                    email, country, postal_code, pet_type, pet_name, pet_breed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_customer_id = EXCLUDED.source_customer_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    age = EXCLUDED.age,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code,
                    pet_type = EXCLUDED.pet_type,
                    pet_name = EXCLUDED.pet_name,
                    pet_breed = EXCLUDED.pet_breed
                """);

        upsertSeller = connection.prepareStatement("""
                INSERT INTO star.dim_seller (
                    seller_key, source_file, source_seller_id, first_name, last_name,
                    email, country, postal_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (seller_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_seller_id = EXCLUDED.source_seller_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code
                """);

        upsertSupplier = connection.prepareStatement("""
                INSERT INTO star.dim_supplier (
                    supplier_key, name, contact, email, phone, address, city, country
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (supplier_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    contact = EXCLUDED.contact,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country
                """);

        upsertProduct = connection.prepareStatement("""
                INSERT INTO star.dim_product (
                    product_key, source_file, source_product_id, supplier_key, name, category,
                    unit_price, available_quantity, pet_category, weight, color, size, brand,
                    material, description, rating, review_count, release_date, expiry_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_product_id = EXCLUDED.source_product_id,
                    supplier_key = EXCLUDED.supplier_key,
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    unit_price = EXCLUDED.unit_price,
                    available_quantity = EXCLUDED.available_quantity,
                    pet_category = EXCLUDED.pet_category,
                    weight = EXCLUDED.weight,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    brand = EXCLUDED.brand,
                    material = EXCLUDED.material,
                    description = EXCLUDED.description,
                    rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
                    release_date = EXCLUDED.release_date,
                    expiry_date = EXCLUDED.expiry_date
                """);

        upsertStore = connection.prepareStatement("""
                INSERT INTO star.dim_store (
                    store_key, name, location, city, state, country, phone, email
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    location = EXCLUDED.location,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email
                """);

        upsertDate = connection.prepareStatement("""
                INSERT INTO star.dim_date (
                    date_key, year, month, day, quarter
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO UPDATE SET
                    year = EXCLUDED.year,
                    month = EXCLUDED.month,
                    day = EXCLUDED.day,
                    quarter = EXCLUDED.quarter
                """);

        upsertFact = connection.prepareStatement("""
                INSERT INTO star.fact_sales (
                    sale_event_id, source_file, source_record_number, source_sale_id, sale_date,
                    customer_key, seller_key, product_key, store_key, supplier_key, quantity, total_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (sale_event_id) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_record_number = EXCLUDED.source_record_number,
                    source_sale_id = EXCLUDED.source_sale_id,
                    sale_date = EXCLUDED.sale_date,
                    customer_key = EXCLUDED.customer_key,
                    seller_key = EXCLUDED.seller_key,
                    product_key = EXCLUDED.product_key,
                    store_key = EXCLUDED.store_key,
                    supplier_key = EXCLUDED.supplier_key,
                    quantity = EXCLUDED.quantity,
                    total_price = EXCLUDED.total_price,
                    loaded_at = NOW()
                """);
    }

    @Override
    public void invoke(SaleEvent event, Context context) throws Exception {
        try {
            bindCustomer(event);
            upsertCustomer.executeUpdate();

            bindSeller(event);
            upsertSeller.executeUpdate();

            bindSupplier(event);
            upsertSupplier.executeUpdate();

            bindProduct(event);
            upsertProduct.executeUpdate();

            bindStore(event);
            upsertStore.executeUpdate();

            LocalDate saleDate = event.saleDate();
            if (saleDate != null) {
                bindDate(saleDate);
                upsertDate.executeUpdate();
            }

            bindFact(event, saleDate);
            upsertFact.executeUpdate();

            connection.commit();
        } catch (Exception exception) {
            rollbackQuietly();
            throw exception;
        }
    }

    @Override
    public void close() throws Exception {
        closeQuietly(upsertFact);
        closeQuietly(upsertDate);
        closeQuietly(upsertStore);
        closeQuietly(upsertProduct);
        closeQuietly(upsertSupplier);
        closeQuietly(upsertSeller);
        closeQuietly(upsertCustomer);
        closeQuietly(connection);
    }

    private void bindCustomer(SaleEvent event) throws SQLException {
        setString(upsertCustomer, 1, event.customerKey());
        setString(upsertCustomer, 2, event.normalizedSourceFile());
        setInteger(upsertCustomer, 3, event.sourceCustomerId());
        setString(upsertCustomer, 4, event.customer_first_name);
        setString(upsertCustomer, 5, event.customer_last_name);
        setInteger(upsertCustomer, 6, event.customerAge());
        setString(upsertCustomer, 7, event.customer_email);
        setString(upsertCustomer, 8, event.customer_country);
        setString(upsertCustomer, 9, event.customer_postal_code);
        setString(upsertCustomer, 10, event.customer_pet_type);
        setString(upsertCustomer, 11, event.customer_pet_name);
        setString(upsertCustomer, 12, event.customer_pet_breed);
    }

    private void bindSeller(SaleEvent event) throws SQLException {
        setString(upsertSeller, 1, event.sellerKey());
        setString(upsertSeller, 2, event.normalizedSourceFile());
        setInteger(upsertSeller, 3, event.sourceSellerId());
        setString(upsertSeller, 4, event.seller_first_name);
        setString(upsertSeller, 5, event.seller_last_name);
        setString(upsertSeller, 6, event.seller_email);
        setString(upsertSeller, 7, event.seller_country);
        setString(upsertSeller, 8, event.seller_postal_code);
    }

    private void bindSupplier(SaleEvent event) throws SQLException {
        setString(upsertSupplier, 1, event.supplierKey());
        setString(upsertSupplier, 2, event.supplier_name);
        setString(upsertSupplier, 3, event.supplier_contact);
        setString(upsertSupplier, 4, event.supplier_email);
        setString(upsertSupplier, 5, event.supplier_phone);
        setString(upsertSupplier, 6, event.supplier_address);
        setString(upsertSupplier, 7, event.supplier_city);
        setString(upsertSupplier, 8, event.supplier_country);
    }

    private void bindProduct(SaleEvent event) throws SQLException {
        setString(upsertProduct, 1, event.productKey());
        setString(upsertProduct, 2, event.normalizedSourceFile());
        setInteger(upsertProduct, 3, event.sourceProductId());
        setString(upsertProduct, 4, event.supplierKey());
        setString(upsertProduct, 5, event.product_name);
        setString(upsertProduct, 6, event.product_category);
        setBigDecimal(upsertProduct, 7, event.productPrice());
        setInteger(upsertProduct, 8, event.productQuantity());
        setString(upsertProduct, 9, event.pet_category);
        setBigDecimal(upsertProduct, 10, event.productWeight());
        setString(upsertProduct, 11, event.product_color);
        setString(upsertProduct, 12, event.product_size);
        setString(upsertProduct, 13, event.product_brand);
        setString(upsertProduct, 14, event.product_material);
        setString(upsertProduct, 15, event.product_description);
        setBigDecimal(upsertProduct, 16, event.productRating());
        setInteger(upsertProduct, 17, event.productReviews());
        setDate(upsertProduct, 18, event.productReleaseDate());
        setDate(upsertProduct, 19, event.productExpiryDate());
    }

    private void bindStore(SaleEvent event) throws SQLException {
        setString(upsertStore, 1, event.storeKey());
        setString(upsertStore, 2, event.store_name);
        setString(upsertStore, 3, event.store_location);
        setString(upsertStore, 4, event.store_city);
        setString(upsertStore, 5, event.store_state);
        setString(upsertStore, 6, event.store_country);
        setString(upsertStore, 7, event.store_phone);
        setString(upsertStore, 8, event.store_email);
    }

    private void bindDate(LocalDate date) throws SQLException {
        setDate(upsertDate, 1, date);
        upsertDate.setShort(2, (short) date.getYear());
        upsertDate.setShort(3, (short) date.getMonthValue());
        upsertDate.setShort(4, (short) date.getDayOfMonth());
        upsertDate.setShort(5, (short) ((date.getMonthValue() - 1) / 3 + 1));
    }

    private void bindFact(SaleEvent event, LocalDate saleDate) throws SQLException {
        setString(upsertFact, 1, event.eventId());
        setString(upsertFact, 2, event.normalizedSourceFile());
        setLong(upsertFact, 3, event.sourceRecordNumber());
        setInteger(upsertFact, 4, event.sourceSaleId());
        setDate(upsertFact, 5, saleDate);
        setString(upsertFact, 6, event.customerKey());
        setString(upsertFact, 7, event.sellerKey());
        setString(upsertFact, 8, event.productKey());
        setString(upsertFact, 9, event.storeKey());
        setString(upsertFact, 10, event.supplierKey());
        setInteger(upsertFact, 11, event.saleQuantity());
        setBigDecimal(upsertFact, 12, event.saleTotalPrice());
    }

    private static void setString(PreparedStatement statement, int index, String value) throws SQLException {
        statement.setString(index, SaleEvent.blankToNull(value));
    }

    private static void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private static void setLong(PreparedStatement statement, int index, Long value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private static void setBigDecimal(PreparedStatement statement, int index, BigDecimal value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.NUMERIC);
        } else {
            statement.setBigDecimal(index, value);
        }
    }

    private static void setDate(PreparedStatement statement, int index, LocalDate value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.DATE);
        } else {
            statement.setDate(index, Date.valueOf(value));
        }
    }

    private void rollbackQuietly() {
        try {
            if (connection != null) {
                connection.rollback();
            }
        } catch (SQLException ignored) {
            // Preserve the original processing error.
        }
    }

    private static void closeQuietly(AutoCloseable closeable) throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }
}
