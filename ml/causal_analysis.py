"""
Методология оценки эффекта господдержки: DID, PSM, Synthetic Control
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import mean_squared_error
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
import warnings
warnings.filterwarnings('ignore')

class CausalAnalysis:
    """Класс для каузального анализа эффекта господдержки"""
    
    def __init__(self, data):
        """
        Инициализация с данными
        
        Parameters:
        data: pd.DataFrame - данные с колонками:
            - org_id: идентификатор организации
            - treatment: бинарная переменная (1 - получили поддержку, 0 - нет)
            - outcome: результат (например, revenue_uplift)
            - pre_outcome: результат до вмешательства
            - covariates: ковариаты для балансировки
        """
        self.data = data.copy()
        self.results = {}
        
    def prepare_data(self, outcome_col, treatment_col, covariate_cols, time_col=None):
        """
        Подготовка данных для анализа
        
        Parameters:
        outcome_col: str - название колонки с результатом
        treatment_col: str - название колонки с лечением
        covariate_cols: list - список ковариат
        time_col: str - название колонки с временем (для панельных данных)
        """
        self.outcome_col = outcome_col
        self.treatment_col = treatment_col
        self.covariate_cols = covariate_cols
        self.time_col = time_col
        
        # Очистка данных
        self.data = self.data.dropna(subset=[outcome_col, treatment_col] + covariate_cols)
        
        # Создание переменных для анализа
        self.data['Y'] = self.data[outcome_col]
        self.data['T'] = self.data[treatment_col]
        self.data['X'] = self.data[covariate_cols]
        
        print(f"Подготовлено {len(self.data)} наблюдений для анализа")
        print(f"Лечение: {self.data['T'].sum()} организаций получили поддержку")
        print(f"Контроль: {(1 - self.data['T']).sum()} организаций не получили поддержку")
        
    def propensity_score_matching(self, caliper=0.1, n_neighbors=1):
        """
        Propensity Score Matching
        
        Parameters:
        caliper: float - максимальное расстояние для матчинга
        n_neighbors: int - количество соседей для матчинга
        """
        print("Выполнение Propensity Score Matching...")
        
        # Оценка propensity score
        X = self.data[self.covariate_cols]
        y = self.data['T']
        
        # Стандартизация ковариат
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Логистическая регрессия
        ps_model = LogisticRegression(random_state=42)
        ps_model.fit(X_scaled, y)
        
        # Получение propensity scores
        self.data['ps'] = ps_model.predict_proba(X_scaled)[:, 1]
        
        # Матчинг
        treated = self.data[self.data['T'] == 1].copy()
        control = self.data[self.data['T'] == 0].copy()
        
        if len(treated) == 0 or len(control) == 0:
            print("Ошибка: нет данных для матчинга")
            return None
        
        # Поиск ближайших соседей
        nbrs = NearestNeighbors(n_neighbors=n_neighbors, radius=caliper)
        nbrs.fit(control[['ps']])
        
        matched_control = []
        matched_treated = []
        
        for i, treated_idx in enumerate(treated.index):
            distances, indices = nbrs.kneighbors([[treated.loc[treated_idx, 'ps']]])
            
            if len(indices[0]) > 0:
                control_idx = control.index[indices[0][0]]
                matched_control.append(control_idx)
                matched_treated.append(treated_idx)
        
        # Создание сбалансированной выборки
        matched_data = pd.concat([
            treated.loc[matched_treated],
            control.loc[matched_control]
        ])
        
        # Оценка эффекта
        treatment_effect = (
            matched_data[matched_data['T'] == 1]['Y'].mean() - 
            matched_data[matched_data['T'] == 0]['Y'].mean()
        )
        
        # Статистическая значимость
        treated_outcomes = matched_data[matched_data['T'] == 1]['Y']
        control_outcomes = matched_data[matched_data['T'] == 0]['Y']
        
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(treated_outcomes, control_outcomes)
        
        self.results['psm'] = {
            'treatment_effect': treatment_effect,
            't_statistic': t_stat,
            'p_value': p_value,
            'n_treated': len(matched_treated),
            'n_control': len(matched_control),
            'matched_data': matched_data
        }
        
        print(f"PSM результат: эффект = {treatment_effect:.2f}, p-value = {p_value:.4f}")
        return self.results['psm']
    
    def difference_in_differences(self, pre_period_col=None, post_period_col=None):
        """
        Difference-in-Differences анализ
        
        Parameters:
        pre_period_col: str - колонка с результатом до вмешательства
        post_period_col: str - колонка с результатом после вмешательства
        """
        print("Выполнение Difference-in-Differences анализа...")
        
        if pre_period_col is None or post_period_col is None:
            print("Ошибка: необходимо указать колонки до и после вмешательства")
            return None
        
        # Подготовка данных для DID
        did_data = self.data.copy()
        did_data['pre_outcome'] = did_data[pre_period_col]
        did_data['post_outcome'] = did_data[post_period_col]
        did_data['time'] = 1  # 1 для пост-периода, 0 для пре-периода
        
        # Создание длинного формата данных
        pre_data = did_data[['org_id', 'T', 'pre_outcome'] + self.covariate_cols].copy()
        pre_data['outcome'] = pre_data['pre_outcome']
        pre_data['time'] = 0
        
        post_data = did_data[['org_id', 'T', 'post_outcome'] + self.covariate_cols].copy()
        post_data['outcome'] = post_data['post_outcome']
        post_data['time'] = 1
        
        long_data = pd.concat([pre_data, post_data], ignore_index=True)
        
        # DID регрессия
        long_data['treatment_time'] = long_data['T'] * long_data['time']
        
        # Добавление ковариат
        X_cols = ['T', 'time', 'treatment_time'] + self.covariate_cols
        X = long_data[X_cols]
        y = long_data['outcome']
        
        # Добавление константы
        X = sm.add_constant(X)
        
        # OLS регрессия
        model = sm.OLS(y, X).fit()
        
        # DID коэффициент
        did_coef = model.params['treatment_time']
        did_se = model.bse['treatment_time']
        did_pvalue = model.pvalues['treatment_time']
        
        self.results['did'] = {
            'treatment_effect': did_coef,
            'standard_error': did_se,
            'p_value': did_pvalue,
            'confidence_interval': [
                did_coef - 1.96 * did_se,
                did_coef + 1.96 * did_se
            ],
            'model_summary': model.summary(),
            'long_data': long_data
        }
        
        print(f"DID результат: эффект = {did_coef:.2f}, p-value = {did_pvalue:.4f}")
        return self.results['did']
    
    def synthetic_control(self, treated_unit, control_units, outcome_col, time_col):
        """
        Synthetic Control Method
        
        Parameters:
        treated_unit: str - идентификатор обработанной единицы
        control_units: list - список идентификаторов контрольных единиц
        outcome_col: str - колонка с результатом
        time_col: str - колонка с временем
        """
        print("Выполнение Synthetic Control анализа...")
        
        # Подготовка данных
        sc_data = self.data[self.data['org_id'].isin([treated_unit] + control_units)].copy()
        
        if len(sc_data) == 0:
            print("Ошибка: нет данных для синтетического контроля")
            return None
        
        # Создание матрицы результатов
        outcome_matrix = sc_data.pivot_table(
            index=time_col, 
            columns='org_id', 
            values=outcome_col
        )
        
        # Разделение на до и после вмешательства
        # Предполагаем, что вмешательство происходит в середине периода
        intervention_time = outcome_matrix.index[len(outcome_matrix) // 2]
        
        pre_period = outcome_matrix[outcome_matrix.index < intervention_time]
        post_period = outcome_matrix[outcome_matrix.index >= intervention_time]
        
        if len(pre_period) == 0 or len(post_period) == 0:
            print("Ошибка: недостаточно данных для синтетического контроля")
            return None
        
        # Обучение синтетического контроля на пре-периоде
        treated_pre = pre_period[treated_unit].values
        control_pre = pre_period[control_units].values
        
        # Минимизация MSE для весов
        from scipy.optimize import minimize
        
        def objective(weights):
            synthetic = np.dot(control_pre, weights)
            return np.mean((treated_pre - synthetic) ** 2)
        
        # Ограничения: веса должны быть неотрицательными и суммироваться в 1
        constraints = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}
        bounds = [(0, 1) for _ in range(len(control_units))]
        
        result = minimize(objective, np.ones(len(control_units)) / len(control_units), 
                         method='SLSQP', bounds=bounds, constraints=constraints)
        
        if not result.success:
            print("Ошибка оптимизации весов")
            return None
        
        weights = result.x
        
        # Создание синтетического контроля
        control_post = post_period[control_units].values
        synthetic_post = np.dot(control_post, weights)
        treated_post = post_period[treated_unit].values
        
        # Оценка эффекта
        treatment_effect = np.mean(treated_post - synthetic_post)
        
        # Доверительный интервал (упрощенный)
        mse = np.mean((treated_post - synthetic_post) ** 2)
        se = np.sqrt(mse / len(treated_post))
        ci_lower = treatment_effect - 1.96 * se
        ci_upper = treatment_effect + 1.96 * se
        
        self.results['synthetic_control'] = {
            'treatment_effect': treatment_effect,
            'standard_error': se,
            'confidence_interval': [ci_lower, ci_upper],
            'weights': dict(zip(control_units, weights)),
            'synthetic_outcome': synthetic_post,
            'treated_outcome': treated_post
        }
        
        print(f"Synthetic Control результат: эффект = {treatment_effect:.2f}")
        return self.results['synthetic_control']
    
    def calculate_roi(self, treatment_effect, support_amount, discount_rate=0.12, horizon=3):
        """
        Расчет ROI господдержки
        
        Parameters:
        treatment_effect: float - эффект от поддержки
        support_amount: float - размер поддержки
        discount_rate: float - ставка дисконтирования
        horizon: int - горизонт расчета (годы)
        """
        # Применение ставки дисконтирования
        discounted_effect = 0
        for year in range(1, horizon + 1):
            discounted_effect += treatment_effect / ((1 + discount_rate) ** year)
        
        # ROI = (Дисконтированный эффект - Поддержка) / Поддержка * 100
        roi = (discounted_effect - support_amount) / support_amount * 100
        
        return {
            'roi_percent': roi,
            'discounted_effect': discounted_effect,
            'support_amount': support_amount,
            'payback_period': support_amount / treatment_effect if treatment_effect > 0 else float('inf')
        }
    
    def generate_report(self):
        """Генерация отчета по каузальному анализу"""
        report = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'data_summary': {
                'total_observations': len(self.data),
                'treated_units': self.data['T'].sum(),
                'control_units': (1 - self.data['T']).sum(),
                'outcome_mean': self.data['Y'].mean(),
                'outcome_std': self.data['Y'].std()
            },
            'results': self.results
        }
        
        return report
    
    def plot_results(self):
        """Визуализация результатов анализа"""
        import matplotlib.pyplot as plt
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # PSM результаты
        if 'psm' in self.results:
            psm_data = self.results['psm']['matched_data']
            axes[0, 0].hist(psm_data[psm_data['T'] == 0]['Y'], alpha=0.7, label='Control', bins=20)
            axes[0, 0].hist(psm_data[psm_data['T'] == 1]['Y'], alpha=0.7, label='Treated', bins=20)
            axes[0, 0].set_title('PSM: Distribution of Outcomes')
            axes[0, 0].legend()
        
        # DID результаты
        if 'did' in self.results:
            did_data = self.results['did']['long_data']
            pre_control = did_data[(did_data['T'] == 0) & (did_data['time'] == 0)]['outcome'].mean()
            pre_treated = did_data[(did_data['T'] == 1) & (did_data['time'] == 0)]['outcome'].mean()
            post_control = did_data[(did_data['T'] == 0) & (did_data['time'] == 1)]['outcome'].mean()
            post_treated = did_data[(did_data['T'] == 1) & (did_data['time'] == 1)]['outcome'].mean()
            
            axes[0, 1].plot(['Pre', 'Post'], [pre_control, post_control], 'o-', label='Control')
            axes[0, 1].plot(['Pre', 'Post'], [pre_treated, post_treated], 'o-', label='Treated')
            axes[0, 1].set_title('DID: Pre vs Post Outcomes')
            axes[0, 1].legend()
        
        # Synthetic Control результаты
        if 'synthetic_control' in self.results:
            sc_result = self.results['synthetic_control']
            axes[1, 0].plot(sc_result['treated_outcome'], label='Treated')
            axes[1, 0].plot(sc_result['synthetic_outcome'], label='Synthetic Control')
            axes[1, 0].set_title('Synthetic Control: Treated vs Synthetic')
            axes[1, 0].legend()
        
        # Сводка результатов
        methods = []
        effects = []
        p_values = []
        
        if 'psm' in self.results:
            methods.append('PSM')
            effects.append(self.results['psm']['treatment_effect'])
            p_values.append(self.results['psm']['p_value'])
        
        if 'did' in self.results:
            methods.append('DID')
            effects.append(self.results['did']['treatment_effect'])
            p_values.append(self.results['did']['p_value'])
        
        if 'synthetic_control' in self.results:
            methods.append('SC')
            effects.append(self.results['synthetic_control']['treatment_effect'])
            p_values.append(0.05)  # Приблизительно
        
        if methods:
            axes[1, 1].bar(methods, effects)
            axes[1, 1].set_title('Treatment Effects by Method')
            axes[1, 1].set_ylabel('Effect Size')
        
        plt.tight_layout()
        plt.show()
        
        return fig
