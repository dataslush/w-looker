Setup
---
## 1. Setup Python Virtualenv and Install requirements.txt
```bash
# 1. Create a Python virtual environment
python3 -m venv venv

# 2. Activate the virtual environment
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

# 3. Install the dependencies from requirements.txt
pip install -r requirements.txt
```

## 2. Add Your Kaggle Credentials under .env file in currenct directory
Check the [sample.env](sample.env) file for same

## 3. Execute below steps to load data to BigQuery

- Below command will check if .env file is properly setup or not
```bash
python app.py check
```

- Below command will download the sales_data_sample.csv file from kaggle
```bash
python app.py download-sales-data
```

- Below command will do some sanity check like looking for null columns and duplicate rows
```bash
python app.py sanity-check
```

- Below command will upload the sales_data_sample.csv file to BigQuery with WRITE_TRUNCATE
```bash
python app.py load-to-bigquery --help
```

If dataset and table doesn't exists this command will create one. Service account should have roles/bigquery.admin access. This is not recommend role,
recommended is to follow the least priviledge policy i.e bigquery.User and bigquery.JobUser role at project level, for the ease we are moving forward with bigquery.admin role.

```bash
python app.py load-to-bigquery dataset-name table-name dataset-location service-account-path.json
```

Assumptions
---

1. The `status` column in our sales table represents the status of each order. Depending on the analysis we want to perform, the "status" field can indeed affect the calculation of total sales and customer purchase patterns. Here's how each status might impact the calculations:

    - **Shipped:** This indicates orders that have been successfully shipped. In most cases, we would include the sales associated with shipped orders in our total sales calculations.

    - **Resolved:** We are assuming that "Resolved" indicates completed orders, we will include these in our total sales calculations.

    - **On Hold:** "On Hold" indicates orders that are temporarily paused or delayed, we will exclude these from total sales until they are no longer on hold.

    - **Cancelled:** Typically, we would exclude the sales associated with cancelled orders from our total sales calculations.

    - **In Process:** "In Process" means the order is still being prepared or is awaiting shipment, we will include it. 

    - **Disputed:** We will choose to exclude them until the dispute is resolved.

    So when we calculate sales we had on year X or quarter X or by customer X we consider, below status only
    - Shipped
    - Resolved
    - In Process

2. We will use the `customername` column to identify unique customers and the `ordernumber` column to distinguish unique orders.
3. The `sales` and `priceeach` values are presented in USD, with the assumption that currency from different countries has been converted into USD.

