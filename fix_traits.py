import os, glob, re

for path in glob.glob('crates/extractors/src/*.rs'):
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    # Remove Extractor implementations
    text = re.sub(r'impl Extractor for \w+ \{.*?\}', '', text, flags=re.MULTILINE|re.DOTALL)
    
    # Remove use statements
    text = re.sub(r'use crate::Extractor;\n?', '', text)
    text = re.sub(r'use domain::models::(?:Match|ClubEloRow);\n?', '', text)
    
    # Remove Extractor trait definition from lib.rs
    text = re.sub(r'pub trait Extractor \{.*?\}', '', text, flags=re.MULTILINE|re.DOTALL)
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
