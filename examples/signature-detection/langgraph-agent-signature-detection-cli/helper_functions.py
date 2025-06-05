import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

SIGNATURE_DATA_DIR = "signature_analysis_data"

def extract_signature_data(result, file_name: str, file_path: str) -> Dict[str, Any]:
    """Extract and structure signature data from TensorLake results"""
    pages = result.outputs.document.pages
    structured_data = {}
    total_signatures = 0

    for page in pages:
        # Find signature fragments
        signature_fragments = [
            frag for frag in page.page_fragments
            if (frag.fragment_type.name.lower() == "signature" and
                frag.content.content.strip().lower() != "no signature detected")
        ]

        if signature_fragments:
            # Extract page content from various fragment types
            page_content = extract_page_content(page.page_fragments)

            structured_data[page.page_number] = {
                "signature_count": len(signature_fragments),
                "bboxes": [frag.bbox for frag in signature_fragments],
                "page_content": page_content,
            }
            total_signatures += len(signature_fragments)

    return {
        "file_name": file_name,
        "file_path": file_path,
        "processed_timestamp": datetime.now().isoformat(),
        "total_signatures": total_signatures,
        "total_pages": len(pages),
        "pages_with_signatures": list(structured_data.keys()),
        "signatures_per_page": structured_data
    }

def extract_page_content(page_fragments: List) -> str:
    """Extract readable content from page fragments"""
    content_parts = []

    for fragment in page_fragments:
        fragment_type = fragment.fragment_type.name.lower()

        if fragment_type == "text":
            content_parts.append(fragment.content.content.strip())
        elif fragment_type == "key_value_region":
            # Prefer markdown for tables if available
            if hasattr(fragment.content, "markdown") and fragment.content.markdown.strip():
                content_parts.append(fragment.content.markdown.strip())
            elif hasattr(fragment.content, "content"):
                content_parts.append(fragment.content.content.strip())

    return "\n\n".join(filter(None, content_parts))


def save_analysis_data(signature_data: Dict[str, Any], file_name: str) -> str:
    """Save signature analysis data to JSON file"""
    safe_filename = "".join(c for c in file_name if c.isalnum() or c in (' ', '-', '_')).strip()
    json_filename = f"{safe_filename}_signature_analysis.json"
    json_path = Path(SIGNATURE_DATA_DIR) / json_filename

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(signature_data, f, indent=2, ensure_ascii=False)

    print(f"Analysis data saved to: {json_path}")
    return str(json_path)
