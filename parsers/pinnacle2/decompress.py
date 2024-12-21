import snappy as lz4  # Заменяем lz4.block на snappy


file_path = 'odds_data/Agudat Sport Nordia Jerusalem FC vs Shimshon Tel Aviv.bin'

try:
    with open(file_path, 'rb') as file:
        # print(file.read())
        compressed_data = file.read()
        decompressed_data = lz4.decompress(compressed_data)
        decompressed_text = decompressed_data.decode('utf-8')
        print(decompressed_text)
except Exception as e:
    print(f"Ошибка декомпрессии: {e}")
except Exception as e:
    print(f"Произошла ошибка: {e}")
