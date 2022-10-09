// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use std::io::Result;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Instant;

use crate::Tpc;

pub struct TpcDs {}

impl TpcDs {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Tpc for TpcDs {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        generator_path: &str,
        output_path: &str,
    ) -> Result<()> {
        let mut handles = vec![];

        let start = Instant::now();

        for i in 1..=partitions {
            let generator_path = generator_path.to_owned();
            let output_path = output_path.to_owned();
            let scale = scale;
            let partitions = partitions;
            handles.push(thread::spawn(move || {
                let output = Command::new("./dsdgen")
                    .current_dir(generator_path.clone())
                    .arg("-FORCE")
                    .arg("-DIR")
                    .arg(output_path)
                    .arg("-SCALE")
                    .arg(format!("{}", scale))
                    .arg("-CHILD")
                    .arg(format!("{}", i))
                    .arg("-PARALLEL")
                    .arg(format!("{}", partitions))
                    .output();

                match output {
                    Ok(x) => println!("{:?}", x),
                    Err(e) => println!(
                        "Failed to build dsdgen command with path {}: {:?}",
                        generator_path, e
                    ),
                }
            }));
        }

        // wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        println!(
            "Generated TPC-DS data at scale factor {} with {} partitions in: {:?}",
            scale, partitions, duration
        );

        let tables = self.get_table_names();

        for table in &tables {
            let output_dir = format!("{}/{}.dat", output_path, table);
            if !Path::new(&output_dir).exists() {
                println!("Creating directory {}", output_dir);
                fs::create_dir(&output_dir)?;
            }
            for i in 1..=partitions {
                let filename = format!("{}/{}_{}_{}.dat", output_path, table, i, partitions);
                let filename2 = format!("{}/part-{}.dat", output_dir, i);
                if Path::new(&filename).exists() {
                    println!("mv {} {}", filename, filename2);
                    fs::rename(filename, filename2)?;
                }
            }
        }

        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec![
            "call_center",
            "catalog_page",
            "catalog_sales",
            "catalog_returns",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "income_band",
            "household_demographics",
            "inventory",
            "store",
            "ship_mode",
            "reason",
            "promotion",
            "item",
            "store_sales",
            "store_returns",
            "web_page",
            "warehouse",
            "time_dim",
            "web_site",
            "web_sales",
            "web_returns",
        ]
    }

    fn get_schema(&self, table: &str) -> Schema {
        match table {
            "customer_address" => Schema::new(vec![
                Field::new("ca_address_sk", DataType::Int32, false),
                Field::new("ca_address_id", DataType::Utf8, false),
                Field::new("ca_street_number", DataType::Utf8, true),
                Field::new("ca_street_name", DataType::Utf8, true),
                Field::new("ca_street_type", DataType::Utf8, true),
                Field::new("ca_suite_number", DataType::Utf8, true),
                Field::new("ca_city", DataType::Utf8, true),
                Field::new("ca_county", DataType::Utf8, true),
                Field::new("ca_state", DataType::Utf8, true),
                Field::new("ca_zip", DataType::Utf8, true),
                Field::new("ca_country", DataType::Utf8, true),
                Field::new("ca_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("ca_location_type", DataType::Utf8, true),
            ]),

            "customer_demographics" => Schema::new(vec![
                Field::new("cd_demo_sk", DataType::Int32, false),
                Field::new("cd_gender", DataType::Utf8, true),
                Field::new("cd_marital_status", DataType::Utf8, true),
                Field::new("cd_education_status", DataType::Utf8, true),
                Field::new("cd_purchase_estimate", DataType::Int32, true),
                Field::new("cd_credit_rating", DataType::Utf8, true),
                Field::new("cd_dep_count", DataType::Int32, true),
                Field::new("cd_dep_employed_count", DataType::Int32, true),
                Field::new("cd_dep_college_count", DataType::Int32, true),
            ]),

            "date_dim" => Schema::new(vec![
                Field::new("d_date_sk", DataType::Int32, false),
                Field::new("d_date_id", DataType::Utf8, false),
                Field::new("d_date", DataType::Date32, true),
                Field::new("d_month_seq", DataType::Int32, true),
                Field::new("d_week_seq", DataType::Int32, true),
                Field::new("d_quarter_seq", DataType::Int32, true),
                Field::new("d_year", DataType::Int32, true),
                Field::new("d_dow", DataType::Int32, true),
                Field::new("d_moy", DataType::Int32, true),
                Field::new("d_dom", DataType::Int32, true),
                Field::new("d_qoy", DataType::Int32, true),
                Field::new("d_fy_year", DataType::Int32, true),
                Field::new("d_fy_quarter_seq", DataType::Int32, true),
                Field::new("d_fy_week_seq", DataType::Int32, true),
                Field::new("d_day_name", DataType::Utf8, true),
                Field::new("d_quarter_name", DataType::Utf8, true),
                Field::new("d_holiday", DataType::Utf8, true),
                Field::new("d_weekend", DataType::Utf8, true),
                Field::new("d_following_holiday", DataType::Utf8, true),
                Field::new("d_first_dom", DataType::Int32, true),
                Field::new("d_last_dom", DataType::Int32, true),
                Field::new("d_same_day_ly", DataType::Int32, true),
                Field::new("d_same_day_lq", DataType::Int32, true),
                Field::new("d_current_day", DataType::Utf8, true),
                Field::new("d_current_week", DataType::Utf8, true),
                Field::new("d_current_month", DataType::Utf8, true),
                Field::new("d_current_quarter", DataType::Utf8, true),
                Field::new("d_current_year", DataType::Utf8, true),
            ]),

            "warehouse" => Schema::new(vec![
                Field::new("w_warehouse_sk", DataType::Int32, false),
                Field::new("w_warehouse_id", DataType::Utf8, false),
                Field::new("w_warehouse_name", DataType::Utf8, true),
                Field::new("w_warehouse_sq_ft", DataType::Int32, true),
                Field::new("w_street_number", DataType::Utf8, true),
                Field::new("w_street_name", DataType::Utf8, true),
                Field::new("w_street_type", DataType::Utf8, true),
                Field::new("w_suite_number", DataType::Utf8, true),
                Field::new("w_city", DataType::Utf8, true),
                Field::new("w_county", DataType::Utf8, true),
                Field::new("w_state", DataType::Utf8, true),
                Field::new("w_zip", DataType::Utf8, true),
                Field::new("w_country", DataType::Utf8, true),
                Field::new("w_gmt_offset", make_decimal_type(5, 2), true),
            ]),

            "ship_mode" => Schema::new(vec![
                Field::new("sm_ship_mode_sk", DataType::Int32, false),
                Field::new("sm_ship_mode_id", DataType::Utf8, false),
                Field::new("sm_type", DataType::Utf8, true),
                Field::new("sm_code", DataType::Utf8, true),
                Field::new("sm_carrier", DataType::Utf8, true),
                Field::new("sm_contract", DataType::Utf8, true),
            ]),

            "time_dim" => Schema::new(vec![
                Field::new("t_time_sk", DataType::Int32, false),
                Field::new("t_time_id", DataType::Utf8, false),
                Field::new("t_time", DataType::Int32, true),
                Field::new("t_hour", DataType::Int32, true),
                Field::new("t_minute", DataType::Int32, true),
                Field::new("t_second", DataType::Int32, true),
                Field::new("t_am_pm", DataType::Utf8, true),
                Field::new("t_shift", DataType::Utf8, true),
                Field::new("t_sub_shift", DataType::Utf8, true),
                Field::new("t_meal_time", DataType::Utf8, true),
            ]),

            "reason" => Schema::new(vec![
                Field::new("r_reason_sk", DataType::Int32, false),
                Field::new("r_reason_id", DataType::Utf8, false),
                Field::new("r_reason_desc", DataType::Utf8, true),
            ]),

            "income_band" => Schema::new(vec![
                Field::new("ib_income_band_sk", DataType::Int32, false),
                Field::new("ib_lower_bound", DataType::Int32, true),
                Field::new("ib_upper_bound", DataType::Int32, true),
            ]),

            "item" => Schema::new(vec![
                Field::new("i_item_sk", DataType::Int32, false),
                Field::new("i_item_id", DataType::Utf8, false),
                Field::new("i_rec_start_date", DataType::Date32, true),
                Field::new("i_rec_end_date", DataType::Date32, true),
                Field::new("i_item_desc", DataType::Utf8, true),
                Field::new("i_current_price", make_decimal_type(7, 2), true),
                Field::new("i_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("i_brand_id", DataType::Int32, true),
                Field::new("i_brand", DataType::Utf8, true),
                Field::new("i_class_id", DataType::Int32, true),
                Field::new("i_class", DataType::Utf8, true),
                Field::new("i_category_id", DataType::Int32, true),
                Field::new("i_category", DataType::Utf8, true),
                Field::new("i_manufact_id", DataType::Int32, true),
                Field::new("i_manufact", DataType::Utf8, true),
                Field::new("i_size", DataType::Utf8, true),
                Field::new("i_formulation", DataType::Utf8, true),
                Field::new("i_color", DataType::Utf8, true),
                Field::new("i_units", DataType::Utf8, true),
                Field::new("i_container", DataType::Utf8, true),
                Field::new("i_manager_id", DataType::Int32, true),
                Field::new("i_product_name", DataType::Utf8, true),
            ]),

            "store" => Schema::new(vec![
                Field::new("s_store_sk", DataType::Int32, false),
                Field::new("s_store_id", DataType::Utf8, false),
                Field::new("s_rec_start_date", DataType::Date32, true),
                Field::new("s_rec_end_date", DataType::Date32, true),
                Field::new("s_closed_date_sk", DataType::Int32, true),
                Field::new("s_store_name", DataType::Utf8, true),
                Field::new("s_number_employees", DataType::Int32, true),
                Field::new("s_floor_space", DataType::Int32, true),
                Field::new("s_hours", DataType::Utf8, true),
                Field::new("s_manager", DataType::Utf8, true),
                Field::new("s_market_id", DataType::Int32, true),
                Field::new("s_geography_class", DataType::Utf8, true),
                Field::new("s_market_desc", DataType::Utf8, true),
                Field::new("s_market_manager", DataType::Utf8, true),
                Field::new("s_division_id", DataType::Int32, true),
                Field::new("s_division_name", DataType::Utf8, true),
                Field::new("s_company_id", DataType::Int32, true),
                Field::new("s_company_name", DataType::Utf8, true),
                Field::new("s_street_number", DataType::Utf8, true),
                Field::new("s_street_name", DataType::Utf8, true),
                Field::new("s_street_type", DataType::Utf8, true),
                Field::new("s_suite_number", DataType::Utf8, true),
                Field::new("s_city", DataType::Utf8, true),
                Field::new("s_county", DataType::Utf8, true),
                Field::new("s_state", DataType::Utf8, true),
                Field::new("s_zip", DataType::Utf8, true),
                Field::new("s_country", DataType::Utf8, true),
                Field::new("s_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("s_tax_precentage", make_decimal_type(5, 2), true),
            ]),

            "call_center" => Schema::new(vec![
                Field::new("cc_call_center_sk", DataType::Int32, false),
                Field::new("cc_call_center_id", DataType::Utf8, false),
                Field::new("cc_rec_start_date", DataType::Date32, true),
                Field::new("cc_rec_end_date", DataType::Date32, true),
                Field::new("cc_closed_date_sk", DataType::Int32, true),
                Field::new("cc_open_date_sk", DataType::Int32, true),
                Field::new("cc_name", DataType::Utf8, true),
                Field::new("cc_class", DataType::Utf8, true),
                Field::new("cc_employees", DataType::Int32, true),
                Field::new("cc_sq_ft", DataType::Int32, true),
                Field::new("cc_hours", DataType::Utf8, true),
                Field::new("cc_manager", DataType::Utf8, true),
                Field::new("cc_mkt_id", DataType::Int32, true),
                Field::new("cc_mkt_class", DataType::Utf8, true),
                Field::new("cc_mkt_desc", DataType::Utf8, true),
                Field::new("cc_market_manager", DataType::Utf8, true),
                Field::new("cc_division", DataType::Int32, true),
                Field::new("cc_division_name", DataType::Utf8, true),
                Field::new("cc_company", DataType::Int32, true),
                Field::new("cc_company_name", DataType::Utf8, true),
                Field::new("cc_street_number", DataType::Utf8, true),
                Field::new("cc_street_name", DataType::Utf8, true),
                Field::new("cc_street_type", DataType::Utf8, true),
                Field::new("cc_suite_number", DataType::Utf8, true),
                Field::new("cc_city", DataType::Utf8, true),
                Field::new("cc_county", DataType::Utf8, true),
                Field::new("cc_state", DataType::Utf8, true),
                Field::new("cc_zip", DataType::Utf8, true),
                Field::new("cc_country", DataType::Utf8, true),
                Field::new("cc_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("cc_tax_percentage", make_decimal_type(5, 2), true),
            ]),

            "customer" => Schema::new(vec![
                Field::new("c_customer_sk", DataType::Int32, false),
                Field::new("c_customer_id", DataType::Utf8, false),
                Field::new("c_current_cdemo_sk", DataType::Int32, true),
                Field::new("c_current_hdemo_sk", DataType::Int32, true),
                Field::new("c_current_addr_sk", DataType::Int32, true),
                Field::new("c_first_shipto_date_sk", DataType::Int32, true),
                Field::new("c_first_sales_date_sk", DataType::Int32, true),
                Field::new("c_salutation", DataType::Utf8, true),
                Field::new("c_first_name", DataType::Utf8, true),
                Field::new("c_last_name", DataType::Utf8, true),
                Field::new("c_preferred_cust_flag", DataType::Utf8, true),
                Field::new("c_birth_day", DataType::Int32, true),
                Field::new("c_birth_month", DataType::Int32, true),
                Field::new("c_birth_year", DataType::Int32, true),
                Field::new("c_birth_country", DataType::Utf8, true),
                Field::new("c_login", DataType::Utf8, true),
                Field::new("c_email_address", DataType::Utf8, true),
                Field::new("c_last_review_date_sk", DataType::Utf8, true),
            ]),

            "web_site" => Schema::new(vec![
                Field::new("web_site_sk", DataType::Int32, false),
                Field::new("web_site_id", DataType::Utf8, false),
                Field::new("web_rec_start_date", DataType::Date32, true),
                Field::new("web_rec_end_date", DataType::Date32, true),
                Field::new("web_name", DataType::Utf8, true),
                Field::new("web_open_date_sk", DataType::Int32, true),
                Field::new("web_close_date_sk", DataType::Int32, true),
                Field::new("web_class", DataType::Utf8, true),
                Field::new("web_manager", DataType::Utf8, true),
                Field::new("web_mkt_id", DataType::Int32, true),
                Field::new("web_mkt_class", DataType::Utf8, true),
                Field::new("web_mkt_desc", DataType::Utf8, true),
                Field::new("web_market_manager", DataType::Utf8, true),
                Field::new("web_company_id", DataType::Int32, true),
                Field::new("web_company_name", DataType::Utf8, true),
                Field::new("web_street_number", DataType::Utf8, true),
                Field::new("web_street_name", DataType::Utf8, true),
                Field::new("web_street_type", DataType::Utf8, true),
                Field::new("web_suite_number", DataType::Utf8, true),
                Field::new("web_city", DataType::Utf8, true),
                Field::new("web_county", DataType::Utf8, true),
                Field::new("web_state", DataType::Utf8, true),
                Field::new("web_zip", DataType::Utf8, true),
                Field::new("web_country", DataType::Utf8, true),
                Field::new("web_gmt_offset", make_decimal_type(5, 2), true),
                Field::new("web_tax_percentage", make_decimal_type(5, 2), true),
            ]),

            "store_returns" => Schema::new(vec![
                Field::new("sr_returned_date_sk", DataType::Int32, true),
                Field::new("sr_return_time_sk", DataType::Int32, true),
                Field::new("sr_item_sk", DataType::Int32, false),
                Field::new("sr_customer_sk", DataType::Int32, true),
                Field::new("sr_cdemo_sk", DataType::Int32, true),
                Field::new("sr_hdemo_sk", DataType::Int32, true),
                Field::new("sr_addr_sk", DataType::Int32, true),
                Field::new("sr_store_sk", DataType::Int32, true),
                Field::new("sr_reason_sk", DataType::Int32, true),
                Field::new("sr_ticket_number", DataType::Int32, false),
                Field::new("sr_return_quantity", DataType::Int32, true),
                Field::new("sr_return_amt", make_decimal_type(7, 2), true),
                Field::new("sr_return_tax", make_decimal_type(7, 2), true),
                Field::new("sr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("sr_fee", make_decimal_type(7, 2), true),
                Field::new("sr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("sr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("sr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("sr_store_credit", make_decimal_type(7, 2), true),
                Field::new("sr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "household_demographics" => Schema::new(vec![
                Field::new("hd_demo_sk", DataType::Int32, false),
                Field::new("hd_income_band_sk", DataType::Int32, true),
                Field::new("hd_buy_potential", DataType::Utf8, true),
                Field::new("hd_dep_count", DataType::Int32, true),
                Field::new("hd_vehicle_count", DataType::Int32, true),
            ]),

            "web_page" => Schema::new(vec![
                Field::new("wp_web_page_sk", DataType::Int32, false),
                Field::new("wp_web_page_id", DataType::Utf8, false),
                Field::new("wp_rec_start_date", DataType::Date32, true),
                Field::new("wp_rec_end_date", DataType::Date32, true),
                Field::new("wp_creation_date_sk", DataType::Int32, true),
                Field::new("wp_access_date_sk", DataType::Int32, true),
                Field::new("wp_autogen_flag", DataType::Utf8, true),
                Field::new("wp_customer_sk", DataType::Int32, true),
                Field::new("wp_url", DataType::Utf8, true),
                Field::new("wp_type", DataType::Utf8, true),
                Field::new("wp_char_count", DataType::Int32, true),
                Field::new("wp_link_count", DataType::Int32, true),
                Field::new("wp_image_count", DataType::Int32, true),
                Field::new("wp_max_ad_count", DataType::Int32, true),
            ]),

            "promotion" => Schema::new(vec![
                Field::new("p_promo_sk", DataType::Int32, false),
                Field::new("p_promo_id", DataType::Utf8, false),
                Field::new("p_start_date_sk", DataType::Int32, true),
                Field::new("p_end_date_sk", DataType::Int32, true),
                Field::new("p_item_sk", DataType::Int32, true),
                Field::new("p_cost", make_decimal_type(15, 2), true),
                Field::new("p_response_target", DataType::Int32, true),
                Field::new("p_promo_name", DataType::Utf8, true),
                Field::new("p_channel_dmail", DataType::Utf8, true),
                Field::new("p_channel_email", DataType::Utf8, true),
                Field::new("p_channel_catalog", DataType::Utf8, true),
                Field::new("p_channel_tv", DataType::Utf8, true),
                Field::new("p_channel_radio", DataType::Utf8, true),
                Field::new("p_channel_press", DataType::Utf8, true),
                Field::new("p_channel_event", DataType::Utf8, true),
                Field::new("p_channel_demo", DataType::Utf8, true),
                Field::new("p_channel_details", DataType::Utf8, true),
                Field::new("p_purpose", DataType::Utf8, true),
                Field::new("p_discount_active", DataType::Utf8, true),
            ]),

            "catalog_page" => Schema::new(vec![
                Field::new("cp_catalog_page_sk", DataType::Int32, false),
                Field::new("cp_catalog_page_id", DataType::Utf8, false),
                Field::new("cp_start_date_sk", DataType::Int32, true),
                Field::new("cp_end_date_sk", DataType::Int32, true),
                Field::new("cp_department", DataType::Utf8, true),
                Field::new("cp_catalog_number", DataType::Int32, true),
                Field::new("cp_catalog_page_number", DataType::Int32, true),
                Field::new("cp_description", DataType::Utf8, true),
                Field::new("cp_type", DataType::Utf8, true),
            ]),

            "inventory" => Schema::new(vec![
                Field::new("inv_date_sk", DataType::Int32, false),
                Field::new("inv_item_sk", DataType::Int32, false),
                Field::new("inv_warehouse_sk", DataType::Int32, false),
                Field::new("inv_quantity_on_hand", DataType::Int32, true),
            ]),

            "catalog_returns" => Schema::new(vec![
                Field::new("cr_returned_date_sk", DataType::Int32, true),
                Field::new("cr_returned_time_sk", DataType::Int32, true),
                Field::new("cr_item_sk", DataType::Int32, false),
                Field::new("cr_refunded_customer_sk", DataType::Int32, true),
                Field::new("cr_refunded_cdemo_sk", DataType::Int32, true),
                Field::new("cr_refunded_hdemo_sk", DataType::Int32, true),
                Field::new("cr_refunded_addr_sk", DataType::Int32, true),
                Field::new("cr_returning_customer_sk", DataType::Int32, true),
                Field::new("cr_returning_cdemo_sk", DataType::Int32, true),
                Field::new("cr_returning_hdemo_sk", DataType::Int32, true),
                Field::new("cr_returning_addr_sk", DataType::Int32, true),
                Field::new("cr_call_center_sk", DataType::Int32, true),
                Field::new("cr_catalog_page_sk", DataType::Int32, true),
                Field::new("cr_ship_mode_sk", DataType::Int32, true),
                Field::new("cr_warehouse_sk", DataType::Int32, true),
                Field::new("cr_reason_sk", DataType::Int32, true),
                Field::new("cr_order_number", DataType::Int32, false),
                Field::new("cr_return_quantity", DataType::Int32, true),
                Field::new("cr_return_amount", make_decimal_type(7, 2), true),
                Field::new("cr_return_tax", make_decimal_type(7, 2), true),
                Field::new("cr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("cr_fee", make_decimal_type(7, 2), true),
                Field::new("cr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("cr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("cr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("cr_store_credit", make_decimal_type(7, 2), true),
                Field::new("cr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "web_returns" => Schema::new(vec![
                Field::new("wr_returned_date_sk", DataType::Int32, true),
                Field::new("wr_returned_time_sk", DataType::Int32, true),
                Field::new("wr_item_sk", DataType::Int32, false),
                Field::new("wr_refunded_customer_sk", DataType::Int32, true),
                Field::new("wr_refunded_cdemo_sk", DataType::Int32, true),
                Field::new("wr_refunded_hdemo_sk", DataType::Int32, true),
                Field::new("wr_refunded_addr_sk", DataType::Int32, true),
                Field::new("wr_returning_customer_sk", DataType::Int32, true),
                Field::new("wr_returning_cdemo_sk", DataType::Int32, true),
                Field::new("wr_returning_hdemo_sk", DataType::Int32, true),
                Field::new("wr_returning_addr_sk", DataType::Int32, true),
                Field::new("wr_web_page_sk", DataType::Int32, true),
                Field::new("wr_reason_sk", DataType::Int32, true),
                Field::new("wr_order_number", DataType::Int32, false),
                Field::new("wr_return_quantity", DataType::Int32, true),
                Field::new("wr_return_amt", make_decimal_type(7, 2), true),
                Field::new("wr_return_tax", make_decimal_type(7, 2), true),
                Field::new("wr_return_amt_inc_tax", make_decimal_type(7, 2), true),
                Field::new("wr_fee", make_decimal_type(7, 2), true),
                Field::new("wr_return_ship_cost", make_decimal_type(7, 2), true),
                Field::new("wr_refunded_cash", make_decimal_type(7, 2), true),
                Field::new("wr_reversed_charge", make_decimal_type(7, 2), true),
                Field::new("wr_account_credit", make_decimal_type(7, 2), true),
                Field::new("wr_net_loss", make_decimal_type(7, 2), true),
            ]),

            "web_sales" => Schema::new(vec![
                Field::new("ws_sold_date_sk", DataType::Int32, true),
                Field::new("ws_sold_time_sk", DataType::Int32, true),
                Field::new("ws_ship_date_sk", DataType::Int32, true),
                Field::new("ws_item_sk", DataType::Int32, false),
                Field::new("ws_bill_customer_sk", DataType::Int32, true),
                Field::new("ws_bill_cdemo_sk", DataType::Int32, true),
                Field::new("ws_bill_hdemo_sk", DataType::Int32, true),
                Field::new("ws_bill_addr_sk", DataType::Int32, true),
                Field::new("ws_ship_customer_sk", DataType::Int32, true),
                Field::new("ws_ship_cdemo_sk", DataType::Int32, true),
                Field::new("ws_ship_hdemo_sk", DataType::Int32, true),
                Field::new("ws_ship_addr_sk", DataType::Int32, true),
                Field::new("ws_web_page_sk", DataType::Int32, true),
                Field::new("ws_web_site_sk", DataType::Int32, true),
                Field::new("ws_ship_mode_sk", DataType::Int32, true),
                Field::new("ws_warehouse_sk", DataType::Int32, true),
                Field::new("ws_promo_sk", DataType::Int32, true),
                Field::new("ws_order_number", DataType::Int32, false),
                Field::new("ws_quantity", DataType::Int32, true),
                Field::new("ws_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ws_list_price", make_decimal_type(7, 2), true),
                Field::new("ws_sales_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("ws_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ws_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("ws_ext_tax", make_decimal_type(7, 2), true),
                Field::new("ws_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("ws_ext_ship_cost", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_ship", make_decimal_type(7, 2), true),
                Field::new("ws_net_paid_inc_ship_tax", make_decimal_type(7, 2), true),
                Field::new("ws_net_profit", make_decimal_type(7, 2), true),
            ]),

            "catalog_sales" => Schema::new(vec![
                Field::new("cs_sold_date_sk", DataType::Int32, true),
                Field::new("cs_sold_time_sk", DataType::Int32, true),
                Field::new("cs_ship_date_sk", DataType::Int32, true),
                Field::new("cs_bill_customer_sk", DataType::Int32, true),
                Field::new("cs_bill_cdemo_sk", DataType::Int32, true),
                Field::new("cs_bill_hdemo_sk", DataType::Int32, true),
                Field::new("cs_bill_addr_sk", DataType::Int32, true),
                Field::new("cs_ship_customer_sk", DataType::Int32, true),
                Field::new("cs_ship_cdemo_sk", DataType::Int32, true),
                Field::new("cs_ship_hdemo_sk", DataType::Int32, true),
                Field::new("cs_ship_addr_sk", DataType::Int32, true),
                Field::new("cs_call_center_sk", DataType::Int32, true),
                Field::new("cs_catalog_page_sk", DataType::Int32, true),
                Field::new("cs_ship_mode_sk", DataType::Int32, true),
                Field::new("cs_warehouse_sk", DataType::Int32, true),
                Field::new("cs_item_sk", DataType::Int32, false),
                Field::new("cs_promo_sk", DataType::Int32, true),
                Field::new("cs_order_number", DataType::Int32, false),
                Field::new("cs_quantity", DataType::Int32, true),
                Field::new("cs_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("cs_list_price", make_decimal_type(7, 2), true),
                Field::new("cs_sales_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("cs_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("cs_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("cs_ext_tax", make_decimal_type(7, 2), true),
                Field::new("cs_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("cs_ext_ship_cost", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_ship", make_decimal_type(7, 2), true),
                Field::new("cs_net_paid_inc_ship_tax", make_decimal_type(7, 2), true),
                Field::new("cs_net_profit", make_decimal_type(7, 2), true),
            ]),

            "store_sales" => Schema::new(vec![
                Field::new("ss_sold_date_sk", DataType::Int32, true),
                Field::new("ss_sold_time_sk", DataType::Int32, true),
                Field::new("ss_item_sk", DataType::Int32, false),
                Field::new("ss_customer_sk", DataType::Int32, true),
                Field::new("ss_cdemo_sk", DataType::Int32, true),
                Field::new("ss_hdemo_sk", DataType::Int32, true),
                Field::new("ss_addr_sk", DataType::Int32, true),
                Field::new("ss_store_sk", DataType::Int32, true),
                Field::new("ss_promo_sk", DataType::Int32, true),
                Field::new("ss_ticket_number", DataType::Int32, false),
                Field::new("ss_quantity", DataType::Int32, true),
                Field::new("ss_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ss_list_price", make_decimal_type(7, 2), true),
                Field::new("ss_sales_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_discount_amt", make_decimal_type(7, 2), true),
                Field::new("ss_ext_sales_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_wholesale_cost", make_decimal_type(7, 2), true),
                Field::new("ss_ext_list_price", make_decimal_type(7, 2), true),
                Field::new("ss_ext_tax", make_decimal_type(7, 2), true),
                Field::new("ss_coupon_amt", make_decimal_type(7, 2), true),
                Field::new("ss_net_paid", make_decimal_type(7, 2), true),
                Field::new("ss_net_paid_inc_tax", make_decimal_type(7, 2), true),
                Field::new("ss_net_profit", make_decimal_type(7, 2), true),
            ]),

            _ => panic!(),
        }
    }

    fn get_table_ext(&self) -> &str {
        "dat"
    }
}

fn make_decimal_type(p: u8, s: u8) -> DataType {
    DataType::Decimal128(p, s)
}
