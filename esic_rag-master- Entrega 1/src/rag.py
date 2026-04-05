import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
def crear_indice(df):
    print("[RAG] Generando embeddings e índice FAISS...")
    embeddings = embedder.encode(df["texto_busqueda"].tolist(), show_progress_bar=False)

    d = embeddings.shape[1]
    index = faiss.IndexFlatL2(d)
    index.add(np.array(embeddings).astype("float32"))

    return index

def buscar_y_responder(consulta, df, index):
    vec_query = embedder.encode([consulta]).astype("float32")
    dist, indices = index.search(vec_query, 15)  
    candidatos = df.iloc[indices[0]].copy()

    # 2. Re-ranking
    max_dist = dist[0].max() if dist[0].max() > 0 else 1
    candidatos["norm_dist"] = 1 - (dist[0] / max_dist)

    # Aplicamos la fórmula:
    candidatos["rank_final"] = (
        candidatos["norm_dist"] * 0.6
        + candidatos["norm_nutri"] * 0.2
        + candidatos["norm_precio"] * 0.2
    )

    # Nos quedamos con los 3 mejores
    mejores = candidatos.sort_values("rank_final", ascending=False).head(3)

    # 3. Formateo de respuesta numerada
    lineas_contexto = []
    for i, (_, r) in enumerate(mejores.iterrows(), 1):
        lineas_contexto.append(
            f"{i}. {r['titulo']} | Precio: {r['precio']}€ | Proteínas: {r['proteinas']}g | Salud: {int(r['score_nutricional'])}/100"
        )
    
    contexto = "\n".join(lineas_contexto)
    respuesta_texto = f"**Asistente Nutricional:** Para '{consulta}', he encontrado estas opciones:\n\n{contexto}"
    return respuesta_texto, mejores

def ejecutar(df):
    index = crear_indice(df)
    carrito = [] 


    print(" BIENVENIDO AL ASISTENTE NUTRICIONAL ")
    print("Puedes buscar productos, escribir 'VER CARRITO' o 'salir'.")

    while True:
        consulta = input("\nIntroduce tu consulta: ").strip()
        
        # Opciones de salida y visualización de carrito
        if consulta.lower() == "salir":
            print("\n¡Hasta luego! Gracias por usar el asistente nutricional.")
            break
            
        elif consulta.upper() == "VER CARRITO":
            if not carrito:
                print("\nTu carrito está vacío en este momento.")
            else:
                print("\n" + "="*40)
                print("TU CARRITO DE LA COMPRA")
                print("="*40)
                total_precio = 0
                total_salud = 0
                
                # Lista productos
                for item in carrito:
                    print(f"- {item['titulo']} | Precio: {item['precio']}€ | Salud: {item['score_nutricional']}/100")
                    total_precio += item['precio']
                    total_salud += item['score_nutricional']
                
                # Salud del carrito
                media_salud = total_salud / len(carrito)
                print("-" * 40)
                print(f"Total a pagar: {total_precio:.2f}€")
                print(f"Salud media de tu compra: {media_salud:.1f}/100")
                print("-" * 40)
                
                if media_salud >= 75:
                    print("¡Excelente! Estás haciendo una compra muy sana y equilibrada.")
                elif media_salud >= 50:
                    print("Tu compra es aceptable, pero intenta incluir más alimentos frescos.")
                else:
                    print("¡Atención! Tu carrito tiene demasiados productos poco saludables.")
                print("="*40)
            continue
            
        respuesta, mejores = buscar_y_responder(consulta, df, index)
        print("\n" + respuesta + "\n")
        
        if mejores.empty:
            print("No se encontraron resultados.")
            continue
            
        # Bucle
        while True:
            anadir = input("¿Deseas añadir algún producto al carrito? (si/no): ").strip().lower()
            
            if anadir in ["no", "n"]:
                break
                
            elif anadir in ["si", "s", "sí"]:
                seleccion = input("Indica el número del producto (1, 2 o 3): ").strip()
                try:
                    idx = int(seleccion) - 1
                    if 0 <= idx < len(mejores):
                        prod = mejores.iloc[idx]
                        carrito.append({
                            'titulo': prod['titulo'],
                            'precio': prod['precio'],
                            'score_nutricional': int(prod['score_nutricional'])
                        })
                        print(f"¡'{prod['titulo']}' añadido al carrito!")
                        break 
                    else:
                        print("Número fuera de rango. Por favor, elige 1, 2 o 3.")
                except ValueError:
                    print("Por favor, introduce un número válido.")
            else:
                print("Respuesta no válida. Por favor, escribe 'si' o 'no'.")