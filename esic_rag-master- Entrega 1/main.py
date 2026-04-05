import os
import sys
import src.acquisition as acquisition
import src.preprocessing as preprocessing
import src.rag as rag


def main():
    """Función principal que orquesta el flujo de trabajo del proyecto."""
    print("=" * 55)
    print("  ASISTENTE NUTRICIONAL DIA — Pipeline RAG")
    print("=" * 55)

    # 1: Obtención de datos
    print("\n[1/3] Iniciando obtención de datos...")

    _HERE = os.path.dirname(os.path.abspath(__file__))
    RAW_PATH = os.path.join(acquisition._HERE,"..", "data", "raw", "productos_dia.json")
    if os.path.exists(RAW_PATH):
        resp = input(
            f"  Ya existe '{RAW_PATH}'. ¿Re-scrapear igualmente? [s/N]: "
        ).strip().lower()
        if resp != "s":
            print("  Usando datos existentes.")
            datos = None  
        else:
            datos = acquisition.obtener_datos(
                paginas_por_categoria=5,   
                delay_crawl=2.0,
                delay_scrape=1.5,
                max_productos=500,
            )
    else:
        datos = acquisition.obtener_datos(
            paginas_por_categoria=5,
            delay_crawl=2.0,
            delay_scrape=1.5,
            max_productos=500,
        )

    # 2: Preprocesamiento
    print("\n[2/3] Iniciando preprocesamiento...")
    try:
        datos_limpios = preprocessing.limpiar_datos(datos)
    except (FileNotFoundError, ValueError) as e:
        print(f"\n[ERROR] Preprocesamiento fallido: {e}")
        sys.exit(1)

    if datos_limpios.empty:
        print("\n[ERROR] El DataFrame procesado está vacío. Revisa los datos crudos.")
        sys.exit(1)
    print(f"  DataFrame listo: {len(datos_limpios)} productos, {len(datos_limpios.columns)} columnas.")

  
    # 3: RAG.
    
    print("\n[3/3] Iniciando sistema RAG...")
    print("  (Generando embeddings — puede tardar unos segundos la primera vez)\n")
    rag.ejecutar(datos_limpios)
    print("\n¡Proceso terminado con éxito!")

if __name__ == "__main__":
    main()


    


