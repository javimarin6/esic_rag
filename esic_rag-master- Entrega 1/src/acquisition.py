import json
import os
import re
import time
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
BASE_DOMAIN = "https://www.dia.es"

CATEGORIAS = {
    "Snacks salados":               "https://www.dia.es/aperitivos-y-frutos-secos/snacks-salados/c/L2282",
    "Aceitunas":                    "https://www.dia.es/aperitivos-y-frutos-secos/aceitunas/c/L2096",
    "Chocolates y bombones":        "https://www.dia.es/azucar-chocolates-y-caramelos/chocolates-y-bombones/c/L2063",
    "Caramelos y golosinas":        "https://www.dia.es/azucar-chocolates-y-caramelos/caramelos-chicles-y-golosinas/c/L2064",
    "Galletas":                     "https://www.dia.es/galletas-bollos-y-cereales/galletas/c/L2065",
    "Bolleria":                     "https://www.dia.es/galletas-bollos-y-cereales/bolleria/c/L2067",
    "Cereales":                     "https://www.dia.es/galletas-bollos-y-cereales/cereales/c/L2068",
    "Jamon cocido":                 "https://www.dia.es/charcuteria-y-quesos/jamon-cocido-pavo-y-pollo/c/L2001",
    "Pizzas":                       "https://www.dia.es/congelados/pizzas-y-masas/c/L2131",
    "Pescado congelado":            "https://www.dia.es/congelados/pescado-y-marisco/c/L2132",
    "Helados":                      "https://www.dia.es/congelados/helados-y-hielo/c/L2130",
    "Salsa de tomate":              "https://www.dia.es/aceites-salsas-y-especias/salsas-de-tomate-y-pasta/c/L2208",
    "Salsas especiales":            "https://www.dia.es/aceites-salsas-y-especias/salsas-especiales-y-picantes/c/L2296",
    "Refresco de cola":             "https://www.dia.es/agua-y-refrescos/cola/c/L2108",
    "Postres infantiles":           "https://www.dia.es/yogures-y-postres/yogures-y-postres-infantiles/c/L2083",
    "Comida tradicional preparada": "https://www.dia.es/platos-preparados-y-pizzas/comida-tradicional/c/L2247",
    "Conservas de verduras":        "https://www.dia.es/verduras/conservas-de-verduras/c/L2026",
    "Batidos":                      "https://www.dia.es/huevos-leche-y-mantequilla/batidos-cafes-frios-y-horchatas/c/L2053",
    "Bebidas vegetales":            "https://www.dia.es/huevos-leche-y-mantequilla/bebidas-vegetales/c/L2052",
    "Cervezas":                     "https://www.dia.es/cervezas-vinos-y-licores/cervezas/c/L2115",
    "Empanados y preparados":       "https://www.dia.es/carnes/empanados-y-elaborados/c/L2265",
    "Carne picada":                 "https://www.dia.es/carnes/hamburguesas-carne-picada-y-albondigas/c/L2017",
    "Pate":                         "https://www.dia.es/charcuteria-y-quesos/pate-foie-y-sobrasada/c/L2012",
    "Ahumados":                     "https://www.dia.es/pescados-y-mariscos/ahumados-y-salazones/c/L2020",

}

_HERE = os.path.dirname(os.path.abspath(__file__))
RAW_PATH = os.path.join(_HERE, "..","data", "raw", "productos_dia.json")


def _fetch_html(url: str, timeout: int = 20) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _to_absolute_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return f"{BASE_DOMAIN}{href}"


def _extract_number(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    try:
        return float(match.group(1)) if match else None
    except ValueError:
        return None


# Crawler:
def _get_product_links(category_url: str, page_end: int = 3, delay: float = 1.0) -> List[str]:
    links = set()
    for page in range(1, page_end + 1):
        url = f"{category_url}?currentPage={page}" if page > 1 else category_url
        try:
            html = _fetch_html(url)
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.select("a.search-product-card__product-image-link"):
                href = a.get("href", "").strip()
                if href:
                    links.add(_to_absolute_url(href))
            if not links:
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if "/p/" in href:
                        links.add(_to_absolute_url(href))

            page_links_count = len(links)
            print(f"    Página {page}: {page_links_count} links acumulados")

            if page_links_count == 0:
                break

        except Exception as e:
            print(f"    [Error crawl página {page}] {e}")
            break

        time.sleep(delay)

    return list(links)


def _get_json_ld(soup: BeautifulSoup) -> Dict:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            content = script.string or script.get_text()
            if not content:
                continue
            data = json.loads(content)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    if item.get("@type") == "Product":
                        return item
                    for sub in item.get("@graph", []):
                        if isinstance(sub, dict) and sub.get("@type") == "Product":
                            return sub
        except Exception:
            continue
    return {}


def _parse_nutricionales(soup: BeautifulSoup) -> Dict[str, str]:
    text = soup.get_text("\n", strip=True)
    lines = [_clean_text(l) for l in text.split("\n") if _clean_text(l)]

    mapa_busqueda = {
        "Proteinas":              ["proteínas", "proteinas"],
        "Hidratos de carbono":    ["hidratos de carbono"],
        "Azucares":               ["azúcares", "azucares"],
        "Grasas":                 ["grasas"],
        "Saturadas":              ["saturadas"],
        "Fibra alimentaria":      ["fibra alimentaria", "fibra"],
        "Sal":                    ["sal"],
        "Valor energetico":       ["valor energético", "valor energetico", "kcal"],
        "Valor energetico en KJ": ["kj"],
    }

    nutri = {}
    patron_valor = re.compile(r"(\d+(?:[.,]\d+)?\s*(?:g|gr|mg|kcal|kj))", re.IGNORECASE)

    for clave, aliases in mapa_busqueda.items():
        for i, line in enumerate(lines):
            lower = line.lower()
            if any(alias in lower for alias in aliases):
                match = patron_valor.search(lower)
                if match:
                    nutri[clave] = _normalizar_cantidad(match.group(1))
                    break
                if i + 1 < len(lines):
                    match = patron_valor.search(lines[i + 1].lower())
                    if match:
                        nutri[clave] = _normalizar_cantidad(match.group(1))
                        break
    return nutri


def _normalizar_cantidad(raw: str) -> str:
    raw = raw.strip().replace(",", ".").replace("gr", "g")
    m = re.match(r"([\d.]+)\s*(kcal|kj|g|mg)", raw, re.IGNORECASE)
    if not m:
        return raw
    num, unit = m.group(1), m.group(2).lower()
    if unit == "kcal": return f"{num} kcal"
    if unit == "kj":   return f"{num} kJ"
    if unit == "g":    return f"{num} gr"
    if unit == "mg":   return f"{num} mg"
    return raw


def _extraer_precio(soup: BeautifulSoup, json_ld: Dict) -> Optional[float]:
    offers = json_ld.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    val = offers.get("price")
    if val:
        try:
            return float(str(val).replace(",", "."))
        except ValueError:
            pass
    el = soup.find("p", class_="buy-box__active-price")
    if el:
        m = re.search(r"(\d+)[,.](\d+)", el.get_text())
        if m:
            return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+[.,]\d+)\s*€", soup.get_text(" ", strip=True))
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def _extraer_alergenos(soup: BeautifulSoup) -> List[str]:
    alergenos = []
    div = soup.find("div", id="html-container")
    if div:
        for strong in div.find_all("strong"):
            t = _clean_text(strong.get_text())
            if t and t not in alergenos:
                alergenos.append(t)
    return alergenos


# Scraper:
def _parse_product(url: str, categoria: str) -> Optional[Dict]:
    try:
        html = _fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")
        json_ld = _get_json_ld(soup)

        # Título
        titulo = _clean_text(json_ld.get("name", ""))
        if not titulo:
            h1 = soup.find("h1")
            titulo = _clean_text(h1.get_text()) if h1 else ""
        if not titulo:
            return None

        # Precio
        precio = _extraer_precio(soup, json_ld)
        if precio is None:
            return None

        # Valores nutricionales
        nutri = _parse_nutricionales(soup)

        if not (nutri.get("Proteinas") and nutri.get("Hidratos de carbono") and nutri.get("Grasas")):
            return None

        # Descripción
        desc_el = soup.find("p", class_="product-summary__description")
        descripcion = _clean_text(desc_el.get_text()) if desc_el else _clean_text(json_ld.get("description", ""))

        # Peso / volumen
        text_full = soup.get_text(" ", strip=True)
        peso = ""
        m = re.search(r"(\d+(?:[.,]\d+)?\s?(?:kg|g|gr|ml|l|cl|ud|uds))", text_full.lower())
        if m:
            peso = m.group(1)

        # Precio por cantidad
        precio_por_cantidad = None
        m = re.search(r"\((\d+[.,]\d+)\s*€/?[A-Za-z]+\)", text_full)
        if m:
            try:
                precio_por_cantidad = float(m.group(1).replace(",", "."))
            except ValueError:
                pass

        return {
            "url":                         url,
            "titulo":                      titulo,
            "descripcion":                 descripcion,
            "categorias":                  [categoria],
            "precio_total":                precio,
            "precio_por_cantidad":         precio_por_cantidad,
            "peso_volumen":                peso,
            "valores_nutricionales_100_g": nutri,
            "alergenos":                   _extraer_alergenos(soup),
            "origen":                      "dia",
        }

    except Exception as e:
        print(f"    [Error] {url}: {e}")
        return None

# Función principal

def obtener_datos(
    paginas_por_categoria: int = 3,
    delay_crawl: float = 1.0,
    delay_scrape: float = 1.0,
    max_productos: int = 300,
) -> List[Dict]:
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)

    # FASE 1: Crawling 
    print("FASE 1: CRAWLING")
    all_urls: Dict[str, List[str]] = {}
    for categoria, base_url in CATEGORIAS.items():
        print(f"\n[Categoría] {categoria}")
        urls = _get_product_links(base_url, page_end=paginas_por_categoria, delay=delay_crawl)
        all_urls[categoria] = urls
        print(f"  → {len(urls)} URLs encontradas")

    total = sum(len(v) for v in all_urls.values())
    print(f"\nTotal URLs a scrapear: {total}")

    # FASE 2: Scraping
    print("FASE 2: SCRAPING")
    productos: List[Dict] = []
    urls_vistas: set = set()

    for categoria, urls in all_urls.items():
        print(f"\n[Categoría] {categoria} ({len(urls)} productos)")
        for i, url in enumerate(urls, 1):
            if url in urls_vistas:
                continue
            urls_vistas.add(url)

            if len(productos) >= max_productos:
                print(f"  Límite de {max_productos} alcanzado.")
                break

            print(f"  [{i}/{len(urls)}] {url}")
            producto = _parse_product(url, categoria)
            if producto:
                tiene_nutri = bool(producto.get("valores_nutricionales_100_g"))
                print(f"  ✓ {producto['titulo']} | {producto['precio_total']}€ | datos nutricionales: {'✓' if tiene_nutri else '✗'}")
                productos.append(producto)
            else:
                print("  ✗ Sin datos")

            time.sleep(delay_scrape)

        if len(productos) >= max_productos:
            break

    # 3: Guardado 
    print(f"GUARDANDO {len(productos)} productos")
    with open(RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(productos, f, ensure_ascii=False, indent=2)
    print(f"Los datos se han guardado en: {RAW_PATH}")

    return productos


if __name__ == "__main__":
    obtener_datos(paginas_por_categoria=3, max_productos=300)