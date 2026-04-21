use jsonwebtoken::decode_header;

fn main() {
    let token = std::env::args().nth(1).expect("Provide token as arg");
    match decode_header(&token) {
        Ok(header) => println!("Header algorithm: {:?}", header.alg),
        Err(e) => println!("Failed to decode header: {:?}", e),
    }
}
