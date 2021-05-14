use std::fs;
use std::io::Result;
use std::path::Path;
use std::process::Command;
use std::thread;

fn main() -> Result<()> {
    //TODO structopt
    let tpcds_kit_path = "/home/andy/git/tpcds-kit/tools";
    let output_path = "/tmp";
    let scale = 1;
    let p = 24;

    let mut handles = vec![];

    for i in 1..=p {
        handles.push(thread::spawn(move || {
            let output = Command::new("./dsdgen")
                .current_dir(tpcds_kit_path)
                .arg("-FORCE")
                .arg("-DIR")
                .arg(output_path)
                .arg("-SCALE")
                .arg(format!("{}", scale))
                .arg("-CHILD")
                .arg(format!("{}", i))
                .arg("-PARALLEL")
                .arg(format!("{}", p))
                .output()
                .expect("failed to generate data");

            println!("{:?}", output);
        }));
    }

    // wait for all threads to finish
    for h in handles {
        h.join().unwrap();
    }

    let tables = [
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
    ];

    for table in &tables {
        let output_dir = format!("{}/{}", output_path, table);
        if !Path::new(&output_dir).exists() {
            println!("Creating directory {}", output_dir);
            fs::create_dir(&output_dir)?;
        }
        for i in 1..=p {
            let filename = format!("{}/{}_{}_{}.dat", output_path, table, i, p);
            let filename2 = format!("{}/part-{}.dat", output_dir, i);
            if Path::new(&filename).exists() {
                println!("mv {} {}", filename, filename2);
                fs::rename(filename, filename2)?;
            }
        }
    }

    Ok(())
}
