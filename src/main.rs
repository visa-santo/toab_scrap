use futures::future::join_all;
use google_sheets4::{api::ValueRange, Sheets};
use hyper::Client;
use hyper_rustls::HttpsConnectorBuilder;
use regex::Regex;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::io::{self, Write};
use std::time::Duration;
use yup_oauth2::ServiceAccountAuthenticator;

// ============================================================
// DATA STRUCTURE
// ============================================================

#[derive(Debug)]
struct MemberData {
    member_id: u32,
    company_name: String,
    phone: String,
    email: String,
    address: String,
    job_position: String,
    name: String,
}

// ============================================================
// ERROR TYPE
// ============================================================

#[derive(Debug)]
enum AppError {
    GoogleApi(String),
    CredentialsNotFound,
    Reqwest(reqwest::Error),
    ParseError(String),
}

impl std::error::Error for AppError {}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::GoogleApi(msg) => {
                write!(f, "Google API Error: {}", msg)
            }

            AppError::CredentialsNotFound => {
                write!(
                    f,
                    "Google credentials not found in environment"
                )
            }

            AppError::Reqwest(e) => {
                write!(f, "Network error: {}", e)
            }

            AppError::ParseError(msg) => {
                write!(f, "Parsing error: {}", msg)
            }
        }
    }
}

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        AppError::Reqwest(err)
    }
}

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ============================================================
    // HTTP CLIENT
    // ============================================================

    let client = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
             AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/120.0.0.0 Safari/537.36",
        )
        .timeout(Duration::from_secs(30))
        .build()?;

    // ============================================================
    // REGEX
    // ============================================================

    // Bangladesh mobile numbers:
    // 013XXXXXXXX
    // 014XXXXXXXX
    // ...
    // 019XXXXXXXX
    //
    // Also handles:
    // +8801XXXXXXXXX
    // 8801XXXXXXXXX
    let phone_regex =
        Regex::new(r"(?:\+?88)?0?(1[3-9]\d{8})")?;

    let email_regex = Regex::new(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    )?;

    // ============================================================
    // GOOGLE SERVICE ACCOUNT
    // ============================================================

    let secret = if let Ok(json_str) =
        env::var("GOOGLE_SERVICE_ACCOUNT_JSON")
    {
        yup_oauth2::parse_service_account_key(json_str)
            .map_err(|e| {
                AppError::GoogleApi(format!(
                    "Failed to parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}"
                ))
            })?
    } else {
        let credentials_path =
            env::var("GOOGLE_APPLICATION_CREDENTIALS")
                .map_err(|_| {
                    AppError::CredentialsNotFound
                })?;

        yup_oauth2::read_service_account_key(
            credentials_path,
        )
        .await
        .map_err(|e| {
            AppError::GoogleApi(format!(
                "Failed to read Google service-account key: {e}"
            ))
        })?
    };

    let auth = ServiceAccountAuthenticator::builder(secret)
        .build()
        .await
        .map_err(|e| {
            AppError::GoogleApi(format!(
                "Failed to initialize Google authenticator: {e}"
            ))
        })?;

    // ============================================================
    // GOOGLE SHEETS HTTPS CONNECTOR
    // ============================================================

    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .build();

    let hub = Sheets::new(
        Client::builder().build(https),
        auth,
    );

    // ============================================================
    // GOOGLE SHEET CONFIGURATION
    // ============================================================

    let spreadsheet_id =
        "1kdC06KMA4JqFkgDod2AptaMJ5i0duWSx8TB7VW1xcu0";

    let sheet_range = "TOAB_SCRAP";

    // ============================================================
    // PROGRAM BANNER
    // ============================================================

    println!("============================================================");
    println!("             TOAB Member Scraper (Google Sheets)            ");
    println!("============================================================");
    println!("Enter Member IDs separated by commas.");
    println!("Example: 16,14,711,370,735");
    println!("Type 'exit' or 'q' to quit.");
    println!();

    // ============================================================
    // MAIN LOOP
    // ============================================================

    loop {
        print!("Enter Member ID(s): ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let trimmed = input.trim();

        // --------------------------------------------------------
        // EXIT
        // --------------------------------------------------------

        if trimmed.eq_ignore_ascii_case("exit")
            || trimmed.eq_ignore_ascii_case("q")
        {
            println!("Exiting program. Goodbye!");
            break;
        }

        // --------------------------------------------------------
        // EMPTY INPUT
        // --------------------------------------------------------

        if trimmed.is_empty() {
            continue;
        }

        // --------------------------------------------------------
        // PARSE IDS
        // --------------------------------------------------------

        let parsed_ids: Vec<u32> = trimmed
            .split(',')
            .filter_map(|value| {
                let id = value.trim().parse::<u32>().ok()?;

                if id == 0 {
                    None
                } else {
                    Some(id)
                }
            })
            .collect();

        if parsed_ids.is_empty() {
            println!("No valid numerical IDs found. Try again.");
            println!();
            continue;
        }

        // --------------------------------------------------------
        // REMOVE DUPLICATE IDS
        // --------------------------------------------------------

        let mut ids = Vec::new();
        let mut seen_ids = HashSet::new();

        for id in parsed_ids {
            if seen_ids.insert(id) {
                ids.push(id);
            }
        }

        println!(
            "Fetching {} member(s) concurrently...",
            ids.len()
        );

        // ========================================================
        // CREATE SCRAPING TASKS
        // ========================================================

        let mut tasks = Vec::new();

        for &member_id in &ids {
            let client_clone = client.clone();
            let phone_regex_clone = phone_regex.clone();
            let email_regex_clone = email_regex.clone();

            tasks.push(tokio::spawn(async move {
                let result = scrape_member(
                    &client_clone,
                    member_id,
                    &phone_regex_clone,
                    &email_regex_clone,
                )
                .await;

                (member_id, result)
            }));
        }

        // ========================================================
        // WAIT FOR ALL TASKS
        // ========================================================

        let results = join_all(tasks).await;

        // ========================================================
        // PREPARE GOOGLE SHEETS ROWS
        //
        // A = Member ID
        // B = Company Name
        // C = Phone
        // D = Email
        // E = Address
        // F = Job Position
        // G = Name
        // ========================================================

        let mut rows_to_append = Vec::new();

        for result in results {
            match result {
                Ok((requested_id, Ok(Some(data)))) => {
                    println!(
                        "[OK] {} -> {} | {} | {} | {} | {} | {}",
                        data.member_id,
                        data.company_name,
                        data.phone,
                        data.email,
                        data.address,
                        data.job_position,
                        data.name
                    );

                    rows_to_append.push(vec![
                        serde_json::Value::String(
                            data.member_id.to_string(),
                        ),
                        serde_json::Value::String(
                            data.company_name,
                        ),
                        serde_json::Value::String(
                            data.phone,
                        ),
                        serde_json::Value::String(
                            data.email,
                        ),
                        serde_json::Value::String(
                            data.address,
                        ),
                        serde_json::Value::String(
                            data.job_position,
                        ),
                        serde_json::Value::String(
                            data.name,
                        ),
                    ]);
                }

                Ok((requested_id, Ok(None))) => {
                    println!(
                        "[SKIP] Member {} -> profile not found",
                        requested_id
                    );
                }

                Ok((requested_id, Err(error))) => {
                    println!(
                        "[ERROR] Member {} -> {}",
                        requested_id,
                        error
                    );
                }

                Err(error) => {
                    println!(
                        "[ERROR] Scraping task failed -> {}",
                        error
                    );
                }
            }
        }

        // ========================================================
        // GOOGLE SHEETS APPEND
        // ========================================================

        if rows_to_append.is_empty() {
            println!("No valid data found to append.");
        } else {
            let row_count = rows_to_append.len();

            println!(
                "Appending {} row(s) to Google Sheets...",
                row_count
            );

            let request = ValueRange {
                values: Some(rows_to_append),
                ..Default::default()
            };

            match hub
                .spreadsheets()
                .values_append(
                    request,
                    spreadsheet_id,
                    sheet_range,
                )
                // RAW is important for +880 phone numbers.
                .value_input_option("RAW")
                .doit()
                .await
            {
                Ok(_) => {
                    println!(
                        "Successfully appended {} row(s) to sheet '{}'.",
                        row_count,
                        sheet_range
                    );
                }

                Err(error) => {
                    println!(
                        "Failed to append to Google Sheets: {}",
                        error
                    );
                }
            }
        }

        println!("--- Batch complete ---");
        println!();
    }

    Ok(())
}

// ============================================================
// SCRAPE ONE TOAB MEMBER
// ============================================================

async fn scrape_member(
    client: &reqwest::Client,
    member_id: u32,
    phone_regex: &Regex,
    email_regex: &Regex,
) -> Result<Option<MemberData>, AppError> {
    // ============================================================
    // CURRENT TOAB MEMBER URL
    // ============================================================

    let url = format!(
        "https://toab.org/toab-members/?member={}",
        member_id
    );

    let response = client.get(&url).send().await?;

    let status = response.status();

    if !status.is_success() {
        println!(
            "[HTTP] Member {} -> HTTP {}",
            member_id,
            status
        );

        return Ok(None);
    }

    let html = response.text().await?;

    // ============================================================
    // PARSE HTML
    // ============================================================

    let document = Html::parse_document(&html);

    // ============================================================
    // EXACT MEMBER PROFILE SECTION
    //
    // Your XPath:
    //
    // /html/body/main/div/div/div/div/div/div/div/div/article/div[2]/section
    //
    // Equivalent useful CSS selector:
    //
    // section.toab-id-profile
    // ============================================================

    let profile_selector =
        Selector::parse("section.toab-id-profile")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create profile selector: {e}"
                ))
            })?;

    let profile_section =
        match document.select(&profile_selector).next() {
            Some(section) => section,

            None => {
                println!(
                    "[SKIP] Member {} -> \
                     section.toab-id-profile not found",
                    member_id
                );

                return Ok(None);
            }
        };

    // ============================================================
    // NAME
    // ============================================================

    let name_selector =
        Selector::parse("#toab-member-name")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create name selector: {e}"
                ))
            })?;

    let name = profile_section
        .select(&name_selector)
        .next()
        .map(|element| {
            normalize_whitespace(
                &element.text().collect::<Vec<_>>().join(" "),
            )
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "N/A".to_string());

    // ============================================================
    // JOB POSITION / DESIGNATION
    // ============================================================

    let designation_selector =
        Selector::parse(".designation")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create designation selector: {e}"
                ))
            })?;

    let job_position = profile_section
        .select(&designation_selector)
        .next()
        .map(|element| {
            normalize_whitespace(
                &element.text().collect::<Vec<_>>().join(" "),
            )
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "N/A".to_string());

    // ============================================================
    // COMPANY NAME
    // ============================================================

    let company_selector =
        Selector::parse(".company")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create company selector: {e}"
                ))
            })?;

    let company_name = profile_section
        .select(&company_selector)
        .next()
        .map(|element| {
            normalize_whitespace(
                &element.text().collect::<Vec<_>>().join(" "),
            )
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "N/A".to_string());

    // ============================================================
    // MEMBERSHIP NUMBER
    // We use the requested ID as fallback.
    // ============================================================

    let metadata_selector =
        Selector::parse("dl.toab-id-meta > div")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create metadata selector: {e}"
                ))
            })?;

    let dt_selector =
        Selector::parse("dt")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create dt selector: {e}"
                ))
            })?;

    let dd_selector =
        Selector::parse("dd")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create dd selector: {e}"
                ))
            })?;

    let mut extracted_member_id = member_id;

    for metadata in
        profile_section.select(&metadata_selector)
    {
        let label = metadata
            .select(&dt_selector)
            .next()
            .map(|element| {
                normalize_whitespace(
                    &element
                        .text()
                        .collect::<Vec<_>>()
                        .join(" "),
                )
            })
            .unwrap_or_default();

        if label.eq_ignore_ascii_case("Membership Number") {
            if let Some(value) =
                metadata.select(&dd_selector).next()
            {
                let value_text = normalize_whitespace(
                    &value.text().collect::<Vec<_>>().join(" "),
                );

                if let Ok(parsed_id) =
                    value_text.parse::<u32>()
                {
                    extracted_member_id = parsed_id;
                }
            }

            break;
        }
    }

    // ============================================================
    // ADDRESS
    // ============================================================

    let contact_selector =
        Selector::parse(".toab-contact-list .toab-contact-item")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create contact selector: {e}"
                ))
            })?;

    let address_selector =
        Selector::parse("span:not(.icon)")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create address selector: {e}"
                ))
            })?;

    let mut address = "N/A".to_string();
    let mut raw_contact_text = String::new();

    for item in
        profile_section.select(&contact_selector)
    {
        let item_text = normalize_whitespace(
            &item.text().collect::<Vec<_>>().join(" "),
        );

        raw_contact_text.push(' ');
        raw_contact_text.push_str(&item_text);

        // Address item is identified by the house icon.
        if item_text.contains('⌂') {
            if let Some(address_span) =
                item.select(&address_selector).next()
            {
                let extracted = normalize_whitespace(
                    &address_span
                        .text()
                        .collect::<Vec<_>>()
                        .join(" "),
                )
                .trim_matches('"')
                .trim()
                .to_string();

                if !extracted.is_empty() {
                    address = extracted;
                }
            } else {
                let fallback = item_text
                    .replace('⌂', "")
                    .trim()
                    .trim_matches('"')
                    .trim()
                    .to_string();

                if !fallback.is_empty() {
                    address = fallback;
                }
            }
        }
    }

    // ============================================================
    // PHONE NUMBERS
    // ============================================================

    let tel_selector =
        Selector::parse("a[href^=\"tel:\"]")
            .map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to create telephone selector: {e}"
                ))
            })?;

    let mut unique_numbers = HashSet::new();

    for link in profile_section.select(&tel_selector) {
        // First try href.
        if let Some(href) = link.value().attr("href") {
            let phone_value = href
                .strip_prefix("tel:")
                .unwrap_or(href)
                .trim();

            for capture in
                phone_regex.captures_iter(phone_value)
            {
                if let Some(number) = capture.get(1) {
                    unique_numbers.insert(format!(
                        "880{}",
                        number.as_str()
                    ));
                }
            }
        }

        // Also inspect visible text as fallback.
        let visible_text = normalize_whitespace(
            &link.text().collect::<Vec<_>>().join(" "),
        );

        for capture in
            phone_regex.captures_iter(&visible_text)
        {
            if let Some(number) = capture.get(1) {
                unique_numbers.insert(format!(
                    "880{}",
                    number.as_str()
                ));
            }
        }
    }

    // If tel links were not found, search contact text.
    if unique_numbers.is_empty() {
        for capture in
            phone_regex.captures_iter(&raw_contact_text)
        {
            if let Some(number) = capture.get(1) {
                unique_numbers.insert(format!(
                    "880{}",
                    number.as_str()
                ));
            }
        }
    }

    let phone = if unique_numbers.is_empty() {
        "N/A".to_string()
    } else {
        let mut numbers: Vec<String> =
            unique_numbers.into_iter().collect();

        numbers.sort();

        numbers.join(", ")
    };
	
// ============================================================
// EMAIL
// ============================================================

let mail_selector =
    Selector::parse("a[href*=\"mailto:\"]")
        .map_err(|e| {
            AppError::ParseError(format!(
                "Failed to create email selector: {e}"
            ))
        })?;

let email = profile_section
    .select(&mail_selector)
    .find_map(|link| {
        link.value()
            .attr("href")
            .and_then(|href| href.strip_prefix("mailto:"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
    })
    .or_else(|| {
        email_regex
            .find(&raw_contact_text)
            .map(|m| m.as_str().to_string())
    })
    .unwrap_or_else(|| "N/A".to_string());

    // ============================================================
    // DEBUG OUTPUT
    // ============================================================

    println!(
        "[OK] Member {} -> Company: {} | Phone: {} | Email: {} | Address: {} | Position: {} | Name: {}",
        extracted_member_id,
        company_name,
        phone,
        email,
        address,
        job_position,
        name
    );

    // ============================================================
    // RETURN DATA
    // ============================================================

    Ok(Some(MemberData {
        member_id: extracted_member_id,
        company_name,
        phone,
        email,
        address,
        job_position,
        name,
    }))
}

// ============================================================
// NORMALIZE WHITESPACE
// ============================================================

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}