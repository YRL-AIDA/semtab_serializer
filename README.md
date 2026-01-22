Функция check_type_comprehensive принимает как одиночные значения, так и целые pd.Series (столбцы).
Она возвращает кортеж, содержащий наибольший тип данных и количество пропусков, найденных в анализируемом наборе. Если большинство за типом int,но есть хотя бы один float, то большинство float.
если функция df_to_duckling не нашла заложенный в ней паттерн, то информацию будет обрабатывать check_type_comprehensive.
<img width="717" height="880" alt="image" src="https://github.com/user-attachments/assets/403ca20b-7194-472c-b649-60adf6179eab" />
