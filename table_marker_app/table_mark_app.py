import streamlit as st
import pandas as pd
import numpy as np
from datasets import load_from_disk
import os
from io import StringIO

# Минималистичная конфигурация страницы
st.set_page_config(
    page_title="Разметка таблиц",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Константы для разметки
COLUMN_NAMES_MAPPING = {"Да": 1, "Нет": 0, "Схематичные": 2}
DESCRIPTION_MAPPING = {"Объемное": 2, "Нет": 0, "Скудное": 1}

def load_dataset(path="dataset"):
    """Минимальная загрузка датасета"""
    if not os.path.exists(path):
        return None
    return load_from_disk(path)

def load_or_create_results(path="results.csv"):
    """Загрузка или создание результатов"""
    if os.path.exists(path):
        df = pd.read_csv(path)
        if 'table_id' not in df.columns:
            df['table_id'] = ''
        if 'mark_columns' not in df.columns:
            df['mark_columns'] = np.nan
        if 'mark_description' not in df.columns:
            df['mark_description'] = np.nan
        return df
    return pd.DataFrame(columns=['table_id', 'mark_columns', 'mark_description'])

def save_results(df, path="results.csv"):
    """Сохранение результатов"""
    df.to_csv(path, index=False)
    return True

def parse_table(text):
    """Парсинг таблицы с разделителем #"""
    try:
        return pd.read_csv(StringIO(text), sep='#', dtype=str, engine='python')
    except:
        # Альтернативный способ
        lines = text.strip().split('\n')
        if lines:
            return pd.DataFrame([line.split('#') for line in lines])
        return pd.DataFrame()

def save_current_annotation(results_df, table_id, col_value, desc_value, results_path):
    """Сохраняет текущую аннотацию в DataFrame и файл"""
    if 'table_id' not in results_df.columns:
        results_df['table_id'] = ''
        results_df['mark_columns'] = np.nan
        results_df['mark_description'] = np.nan
    
    mask = results_df['table_id'] == table_id
    if mask.any():
        idx = results_df.index[mask].tolist()[0]
        if col_value is not None:
            results_df.at[idx, 'mark_columns'] = col_value
        if desc_value is not None:
            results_df.at[idx, 'mark_description'] = desc_value
    else:
        results_df = pd.concat([results_df, pd.DataFrame([{
            'table_id': table_id,
            'mark_columns': col_value,
            'mark_description': desc_value
        }])], ignore_index=True)
    
    save_results(results_df, results_path)
    return results_df

def main():
    # Минимальный заголовок
    st.title("📊 Разметка таблиц")
    
    # Настройки в компактном виде
    with st.expander("Настройки", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            dataset_path = st.text_input("Путь к датасету", "dataset")
        with col2:
            results_path = st.text_input("Файл результатов", "results.csv")
        
        if st.button("Загрузить", type="primary"):
            st.rerun()
    
    # Загрузка данных
    dataset = load_dataset(dataset_path)
    if dataset is None:
        st.error(f"Датасет не найден: {dataset_path}")
        st.info("Создайте папку 'dataset' или укажите путь в настройках")
        return
    
    results_df = load_or_create_results(results_path)
    
    # Инициализация состояния
    if 'current_index' not in st.session_state:
        st.session_state.current_index = 0
    if 'total_tables' not in st.session_state:
        st.session_state.total_tables = len(dataset)
    
    # Функции навигации с сохранением
    def go_prev():
        """Переход к предыдущей таблице с сохранением"""
        current_idx = st.session_state.current_index
        record = dataset[current_idx]
        table_id = f"{current_idx}_{record['table']}"
        
        # Сохраняем текущие значения если они есть
        current_cols = st.session_state.get(f"current_cols_{current_idx}")
        current_desc = st.session_state.get(f"current_desc_{current_idx}")
        
        if current_cols is not None or current_desc is not None:
            results_df = load_or_create_results(results_path)
            save_current_annotation(results_df, table_id, current_cols, current_desc, results_path)
        
        # Переходим к предыдущей
        st.session_state.current_index = max(0, current_idx - 1)
        st.rerun()
    
    def go_next():
        """Переход к следующей таблице с сохранением"""
        current_idx = st.session_state.current_index
        record = dataset[current_idx]
        table_id = f"{current_idx}_{record['table']}"
        
        # Сохраняем текущие значения если они есть
        current_cols = st.session_state.get(f"current_cols_{current_idx}")
        current_desc = st.session_state.get(f"current_desc_{current_idx}")
        
        if current_cols is not None or current_desc is not None:
            results_df = load_or_create_results(results_path)
            save_current_annotation(results_df, table_id, current_cols, current_desc, results_path)
        
        # Переходим к следующей
        st.session_state.current_index = min(st.session_state.total_tables - 1, current_idx + 1)
        st.rerun()
    
    def go_to_table(target_idx_input):
        """Переход к указанной таблице с сохранением"""
        current_idx = st.session_state.current_index
        target_idx = target_idx_input - 1  # Пользователь вводит начиная с 1
        
        if target_idx != current_idx and 0 <= target_idx < st.session_state.total_tables:
            # Сохраняем текущие значения
            record = dataset[current_idx]
            table_id = f"{current_idx}_{record['table']}"
            
            current_cols = st.session_state.get(f"current_cols_{current_idx}")
            current_desc = st.session_state.get(f"current_desc_{current_idx}")
            
            if current_cols is not None or current_desc is not None:
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, current_cols, current_desc, results_path)
            
            # Переходим к целевой таблице
            st.session_state.current_index = target_idx
            st.rerun()
    
    # Компактная панель навигации
    col1, col2, col3, col4 = st.columns([1, 2, 1, 2])
    
    with col1:
        if st.button("←", use_container_width=True, key="prev_btn"):
            go_prev()
    
    with col2:
        # Создаем состояние для целевого индекса
        if 'target_index_input' not in st.session_state:
            st.session_state.target_index_input = st.session_state.current_index + 1
        
        # Поле ввода номера таблицы
        target_index = st.number_input(
            "Таблица",
            min_value=1,
            max_value=st.session_state.total_tables,
            value=st.session_state.current_index + 1,
            step=1,
            label_visibility="collapsed",
            key="table_input"
        )
        
        # Проверяем, изменилось ли значение
        if target_index != st.session_state.target_index_input:
            st.session_state.target_index_input = target_index
            go_to_table(target_index)
    
    with col3:
        if st.button("→", use_container_width=True, key="next_btn"):
            go_next()
    
    with col4:
        # Отображаем актуальный прогресс
        current_idx = st.session_state.current_index
        progress_text = f"Прогресс: {current_idx + 1}/{st.session_state.total_tables}"
        st.caption(progress_text)
    
    # Текущая запись
    current_idx = st.session_state.current_index
    record = dataset[current_idx]
    table_id = f"{current_idx}_{record['table']}"
    
    # Получение текущей разметки из файла
    saved_mark = {'columns': None, 'description': None}
    if 'table_id' in results_df.columns and not results_df.empty:
        match = results_df[results_df['table_id'] == table_id]
        if not match.empty:
            saved_mark['columns'] = match.iloc[0]['mark_columns']
            saved_mark['description'] = match.iloc[0]['mark_description']
    
    # Инициализация текущих значений в session_state
    if f"current_cols_{current_idx}" not in st.session_state:
        st.session_state[f"current_cols_{current_idx}"] = saved_mark['columns']
    
    if f"current_desc_{current_idx}" not in st.session_state:
        st.session_state[f"current_desc_{current_idx}"] = saved_mark['description']
    
    # Получаем текущие значения
    current_cols = st.session_state[f"current_cols_{current_idx}"]
    current_desc = st.session_state[f"current_desc_{current_idx}"]
    
    # Основное содержание - две колонки
    col_left, col_right = st.columns([7, 3])
    
    with col_left:
        # Таблица и ее описание
        st.subheader("Таблица")
        
        # Отображаем описание таблицы (если есть) прямо под заголовком
        if record.get('table_descr') and str(record['table_descr']).strip():
            # Создаем контейнер для описания с темным фоном и светлым текстом
            description_html = f"""
            <div style="
                background-color: #2c3e50;
                color: #ecf0f1;
                padding: 12px 16px;
                border-radius: 6px;
                border-left: 5px solid #3498db;
                margin-bottom: 20px;
                font-size: 14px;
                line-height: 1.5;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            ">
            <strong style="color: #3498db; font-size: 15px;">📝 Описание таблицы:</strong><br>
            <div style="margin-top: 8px; color: #ecf0f1;">
            {record['table_descr']}
            </div>
            </div>
            """
            st.markdown(description_html, unsafe_allow_html=True)
        else:
            # Если описания нет, показываем серую плашку
            st.markdown(
                """
                <div style="
                    background-color: #f5f5f5;
                    color: #666;
                    padding: 12px 16px;
                    border-radius: 6px;
                    border-left: 5px solid #95a5a6;
                    margin-bottom: 20px;
                    font-size: 14px;
                    font-style: italic;
                ">
                <strong>Описание таблицы:</strong> отсутствует
                </div>
                """,
                unsafe_allow_html=True
            )
        
        # Сама таблица
        table_df = parse_table(record['table_text'])
        if not table_df.empty:
            height = min(400, 35 * len(table_df) + 50)
            st.dataframe(table_df, use_container_width=True, height=height)
        else:
            st.warning("Таблица не распознана")
            preview = record['table_text'][:500] + "..." if len(record['table_text']) > 500 else record['table_text']
            st.code(preview)
    
    with col_right:
        # Разметка
        st.subheader("Разметка")
        
        # Названия столбцов
        st.markdown("**Названия столбцов:**")
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            col_0_pressed = st.button("Нет", key=f"col_0_{current_idx}", use_container_width=True,
                                    type="primary" if current_cols == 0 else "secondary")
            if col_0_pressed:
                st.session_state[f"current_cols_{current_idx}"] = 0
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, 0, current_desc, results_path)
                st.rerun()
        
        with col_btn2:
            col_1_pressed = st.button("Да", key=f"col_1_{current_idx}", use_container_width=True,
                                    type="primary" if current_cols == 1 else "secondary")
            if col_1_pressed:
                st.session_state[f"current_cols_{current_idx}"] = 1
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, 1, current_desc, results_path)
                st.rerun()
        
        with col_btn3:
            col_2_pressed = st.button("Схем.", key=f"col_2_{current_idx}", use_container_width=True,
                                    type="primary" if current_cols == 2 else "secondary")
            if col_2_pressed:
                st.session_state[f"current_cols_{current_idx}"] = 2
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, 2, current_desc, results_path)
                st.rerun()
        
        # Описание таблицы (для разметки)
        st.markdown("**Описание таблицы:**")
        desc_btn1, desc_btn2, desc_btn3 = st.columns(3)
        
        with desc_btn1:
            desc_0_pressed = st.button("Нет", key=f"desc_0_{current_idx}", use_container_width=True,
                                     type="primary" if current_desc == 0 else "secondary")
            if desc_0_pressed:
                st.session_state[f"current_desc_{current_idx}"] = 0
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, current_cols, 0, results_path)
                st.rerun()
        
        with desc_btn2:
            desc_1_pressed = st.button("Скуд.", key=f"desc_1_{current_idx}", use_container_width=True,
                                     type="primary" if current_desc == 1 else "secondary")
            if desc_1_pressed:
                st.session_state[f"current_desc_{current_idx}"] = 1
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, current_cols, 1, results_path)
                st.rerun()
        
        with desc_btn3:
            desc_2_pressed = st.button("Объем.", key=f"desc_2_{current_idx}", use_container_width=True,
                                     type="primary" if current_desc == 2 else "secondary")
            if desc_2_pressed:
                st.session_state[f"current_desc_{current_idx}"] = 2
                results_df = load_or_create_results(results_path)
                save_current_annotation(results_df, table_id, current_cols, 2, results_path)
                st.rerun()
        
        # Показываем текущие значения разметки
        st.markdown("---")
        if current_cols is not None:
            status_text = {0: "Нет", 1: "Да", 2: "Схематичные"}.get(current_cols, "Не выбрано")
            st.info(f"**Столбцы:** {status_text}")
        else:
            st.warning("**Столбцы:** не размечены")
        
        if current_desc is not None:
            status_text = {0: "Нет", 1: "Скудное", 2: "Объемное"}.get(current_desc, "Не выбрано")
            st.info(f"**Описание:** {status_text}")
        else:
            st.warning("**Описание:** не размечено")
        
        # Статистика
        st.markdown("---")
        marked_count = results_df['mark_columns'].notna().sum()
        st.caption(f"Всего размечено: {marked_count}/{st.session_state.total_tables}")
        
        # Кнопка сохранения (для перестраховки)
        if st.button("💾 Сохранить", use_container_width=True, type="primary", key="save_btn"):
            results_df = load_or_create_results(results_path)
            save_current_annotation(results_df, table_id, current_cols, current_desc, results_path)
            st.success("✓ Сохранено!")
            st.rerun()
        
        # Скачивание результатов
        if not results_df.empty:
            csv = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать CSV",
                data=csv,
                file_name="results.csv",
                mime="text/csv",
                use_container_width=True,
                key="download_btn"
            )
    
    # Горячие клавиши
    js = """
    <script>
    document.addEventListener('keydown', function(e) {
        // Навигация
        if (e.key === 'ArrowLeft') {
            const prevBtn = window.parent.document.querySelector('button[data-testid="baseButton-secondary"][aria-label*="←"]');
            if (prevBtn) prevBtn.click();
        }
        if (e.key === 'ArrowRight') {
            const nextBtn = window.parent.document.querySelector('button[data-testid="baseButton-secondary"][aria-label*="→"]');
            if (nextBtn) nextBtn.click();
        }
        
        // Сохранение
        if (e.key === 's' || e.key === 'S') {
            const saveBtn = window.parent.document.querySelector('button[data-testid="baseButton-primary"]');
            if (saveBtn && saveBtn.textContent.includes('Сохранить')) {
                saveBtn.click();
            }
        }
    });
    </script>
    """
    st.components.v1.html(js, height=0)

if __name__ == "__main__":
    main()