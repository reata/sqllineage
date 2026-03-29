# a Kimball dimensional model, source from https://github.com/Data-Engineer-Camp/dbt-dimensional-modelling
data_warehouse_schemas = {
    # raw data, Third Normal Form (3NF) modeling
    "date.date": [
        "date_day",  # date, PK
        "prior_date_day",  # date
        "next_date_day",  # date
        "prior_year_date_day",  # date
        "prior_year_over_year_date_day",  # date
        "day_of_week",  # integer
        "day_of_week_name",  # varchar
        "day_of_month",  # integer
        "day_of_year",  # integer
    ],
    "person.address": [
        "addressid",  # integer, PK
        "addressline1",  # varchar
        "addressline2",  # varchar
        "city",  # varchar
        "stateprovinceid",  # integer, FK to person.stateprovince.stateprovinceid
        "postalcode",  # varchar
        "spatiallocation",  # varchar
        "rowguid",  # varchar
        "modifieddate",  # timestamp
    ],
    "person.countryregion": [
        "countryregioncode",  # varchar, PK
        "modifieddate",  # timestamp
        "name",  # varchar
    ],
    "person.person": [
        "businessentityid",  # integer, PK
        "title",  # varchar
        "firstname",  # varchar
        "middlename",  # varchar
        "lastname",  # varchar
        "persontype",  # varchar
        "namestyle",  # boolean
        "suffix",  # varchar
        "modifieddate",  # timestamp
        "rowguid",  # varchar
        "emailpromotion",  # integer
    ],
    "person.stateprovince": [
        "stateprovinceid",  # integer, PK
        "countryregioncode",  # varchar, FK to person.countryregion.countryregioncode
        "modifieddate",  # timestamp
        "rowguid",  # varchar
        "name",  # varchar
        "territoryid",  # integer
        "isonlystateprovinceflag",  # boolean
        "stateprovincecode",  # varchar
    ],
    "production.product": [
        "productid",  # integer, PK
        "name",  # varchar
        "safetystocklevel",  # smallint
        "finishedgoodsflag",  # boolean
        "class",  # varchar
        "makeflag",  # boolean
        "productnumber",  # varchar
        "reorderpoint",  # smallint
        "modifieddate",  # timestamp
        "rowguid",  # varchar
        "productmodelid",  # integer
        "weightunitmeasurecode",  # varchar
        "standardcost",  # numeric
        "productsubcategoryid",  # integer, FK to production.productsubcategory.productsubcategoryid
        "listprice",  # numeric
        "daystomanufacture",  # integer
        "productline",  # varchar
        "color",  # varchar
        "sellstartdate",  # timestamp
        "weight",  # numeric
    ],
    "production.productcategory": [
        "productcategoryid",  # integer, PK
        "name",  # varchar
        "modifieddate",  # timestamp
    ],
    "production.productsubcategory": [
        "productsubcategoryid",  # integer, PK
        "productcategoryid",  # integer, FK to production.productcategory.productcategoryid
        "name",  # varchar
        "modifieddate",  # timestamp
    ],
    "sales.creditcard": [
        "creditcardid",  # integer, PK
        "cardtype",  # varchar
        "expyear",  # smallint
        "modifieddate",  # timestamptz
        "expmonth",  # smallint
        "cardnumber",  # varchar
    ],
    "sales.customer": [
        "customerid",  # integer
        "personid",  # integer, FK to person.person.businessentityid
        "storeid",  # integer, FK to sales.store.businessentityid
        "territoryid",  # integer
    ],
    "sales.salesorderdetail": [
        "salesorderdetailid",  # integer, PK
        "salesorderid",  # integer, FK to sales.salesorderheader.salesorderid
        "orderqty",  # smallint
        "unitprice",  # numeric
        "specialofferid",  # integer
        "modifieddate",  # timestamp
        "rowguid",  # varchar
        "productid",  # integer, FK to production.product.productid
        "unitpricediscount",  # numeric
    ],
    "sales.salesorderheader": [
        "salesorderid",  # integer, PK
        "shipmethodid",  # integer
        "billtoaddressid",  # integer
        "modifieddate",  # timestamp
        "rowguid",  # varchar
        "taxamt",  # numeric
        "shiptoaddressid",  # integer, FK to person.address.addressid
        "onlineorderflag",  # boolean
        "territoryid",  # integer
        "status",  # smallint
        "orderdate",  # timestamp
        "creditcardapprovalcode",  # varchar
        "subtotal",  # numeric
        "creditcardid",  # integer, FK to sales.creditcard
        "currencyrateid",  # integer
        "revisionnumber",  # smallint
        "freight",  # numeric
        "duedate",  # timestamp
        "totaldue",  # numeric
        "customerid",  # integer, FK to sales.customer.customerid
        "salespersonid",  # integer
        "shipdate",  # timestamp
        "accountnumber",  # varchar
    ],
    "sales.salesorderheadersalesreason": [
        "salesorderid",  # integer, FK to sales.salesorderheader.salesorderid
        "modifieddate",  # timestamp
        "salesreasonid",  # integer, FK to sales.salesreason.salesreasonid
    ],
    "sales.salesreason": [
        "salesreasonid",  # integer, PK
        "name",  # varchar
        "reasontype",  # varchar
        "modifieddate",  # timestamp
    ],
    "sales.store": [
        "businessentityid",  # integer, PK
        "storename",  # varchar
        "salespersonid",  # integer
        "modifieddate",  # timestamp
    ],
    # data mart, Dimensional modeling
    "marts.dim_address": [
        "address_key",  # The surrogate key of the addressid
        "addressid",  # The natural key
        "city_name",  # The city name
        "state_name",  # The state name
        "country_name",  # The country name
    ],
    "marts.dim_credit_card": [
        "creditcard_key",  # The surrogate key of the creditcard id
        "creditcardid",  # The natural key of the creditcard
        "cardtype",  # The card name
    ],
    "marts.dim_customer": [
        "customer_key",  # The surrogate key of the customer
        "customerid",  # The natural key of the customer
        "businessentityid",
        "fullname",  # The customer name. Adopted as customer_fullname when person name is not null.
        "storebusinessentityid",
        "storename",  # The store name.
    ],
    "marts.dim_date": [
        "date_key",  # The surrogate key of the date table
        "date_day",  # The natural key of the date table
        "prior_date_day",  # date
        "next_date_day",  # date
        "prior_year_date_day",  # date
        "prior_year_over_year_date_day",  # date
        "day_of_week",  # integer
        "day_of_week_name",  # varchar
        "day_of_month",  # integer
        "day_of_year",  # integer
    ],
    "marts.dim_order_status": [
        "order_status_key",  # The surrogate key of the order status
        "order_status",  # The natural key of the order status table
        "order_status_name",  # varchar
    ],
    "marts.dim_product": [
        "product_key",  # The surrogate key of the product
        "productid",  # The natural key of the product
        "product_name",  # The product name
        "productnumber",  # varchar
        "color",  # varchar
        "class",  # varchar
        "product_subcategory_name",  # varchar
        "product_category_name",  # varchar
    ],
    "marts.fct_sales": [
        "sales_key",  # The surrogate key of the fct sales
        "product_key",  # The foreign key of the product
        "customer_key",  # The foreign key of the customer
        "creditcard_key",  # The foreign key of the creditcard. If no creditcard exists, it was assumed that purchase
        # was made in cash
        "ship_address_key",  # The foreign key of the shipping address
        "order_status_key",  # The foreign key of the order status
        "order_date_key",  # The foreign key of the order date
        "salesorderid",  # The natural key of the saleorderheader
        "salesorderdetailid",  # The natural key of the salesorderdetail
        "unitprice",  # The unit price of the product
        "orderqty",  # The quantity of the product
        "revenue",  # The revenue obtained by multiplying unitprice and orderqty
    ],
    # data mart, One Big Table (OBT) modeling
    "marts.obt_sales": [
        "sales_key",  # The surrogate key of the fct sales
        "salesorderid",  # The natural key of the saleorderheader
        "salesorderdetailid",  # The natural key of the salesorderdetail
        "unitprice",  # The unit price of the product
        "orderqty",  # The quantity of the product
        "revenue",  # The revenue obtained by multiplying unitprice and orderqty
        "productid",  # The natural key of the product
        "product_name",  # The product name
        "productnumber",  # varchar
        "color",  # varchar
        "class",  # varchar
        "product_subcategory_name",  # varchar
        "product_category_name",  # varchar
        "customerid",  # The natural key of the customer
        "businessentityid",
        "fullname",  # The customer name. Adopted as customer_fullname when person name is not null.
        "storebusinessentityid",
        "storename",  # The store name.
        "creditcardid",  # The natural key of the creditcard
        "cardtype",  # The card name
        "addressid",  # The natural key
        "city_name",  # The city name
        "state_name",  # The state name
        "country_name",  # The country name
        "order_status",  # The natural key of the order status table
        "order_status_name",  # varchar
        "date_day",  # The natural key of the date table
        "prior_date_day",  # date
        "next_date_day",  # date
        "prior_year_date_day",  # date
        "prior_year_over_year_date_day",  # date
        "day_of_week",  # integer
        "day_of_week_name",  # varchar
        "day_of_month",  # integer
        "day_of_year",  # integer
    ],
}
