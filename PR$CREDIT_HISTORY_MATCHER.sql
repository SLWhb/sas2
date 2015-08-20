CREATE OR REPLACE PROCEDURE PR$CREDIT_HISTORY_MATCHER
-- ======================================================================== --
-- == ПРОЦЕДУРА "ДЕДУБЛИКАЦИЯ ЗАПИСЕЙ КРЕДИТНОЙ ИСТОРИИ" v2.2            == --
-- ======================================================================== --
-- == СОЗДАНИЕ: 19.06.2012 (КОЗЛОВ И.Я.)                                 == --
-- == МОДИФИКАЦИЯ: 17.02.2014 (КОЗЛОВ И.Я.)                              == --
-- ======================================================================== --
IS
  -- Новый тип для групп
  TYPE T_GROUPS IS TABLE OF TYPE_T_NUMBER;

  -- Коллекция групп наборов дублей
  colGroups           T_GROUPS := T_GROUPS();
  -- Коллекция рассчитанных параметров записей КИ (DOUBLE_ID СТРОКА_КАЧЕСТВА ВЕС ДОСТОВЕРНОСТЬ)
  colActualMop        T_ACTUAL_MOP := T_ACTUAL_MOP();
  -- Коллекция дублей записей КИ
  colDouble           TYPE_T_NUMBER := TYPE_T_NUMBER();
  -- Коллекция приращения групп
  vSplit_group        TYPE_T_NUMBER := TYPE_T_NUMBER();
  -- Коллекция атрибутов для определения суммы аннуитета
  colPayment          T_PAYMENT := T_PAYMENT();
  -- Объявляем переменные
  nRequest_id         INT;
  nRequest_react_id   INT;
  nPerson_id          INT;
  -- Набор статусов для определения кандидатов
  vRowStatus_id       VARCHAR2(4000);
  -- Атрибуты
  aSummaFull          NUMBER;
  aDateBegin          NUMBER;
  aDateEnd            NUMBER;
  aSummaAnn           NUMBER;
  aCurrency           NUMBER;
  -- Дистанция
  dSummaFull          NUMBER;
  dDateBegin          NUMBER;
  dDateEnd            NUMBER;
  dSummaAnn           NUMBER;
  dCurrency           NUMBER;
  -- Порог для попадания записей в дубли
  nBall               NUMBER;
  -- Результат сравнения
  nCompareResult      NUMBER;
  -- Рассчитанный срок обслуживания
  nCalc_credit_period INT;
  -- Кол-во кредитов ВЭБ в группе
  nCnt_veb            PLS_INTEGER;
  -- Кол-во нулевых аннуитетов в группе
  nCnt_zero           PLS_INTEGER;
  -- Флаг добавления группы
  bAdd_group          BOOLEAN;
  -- Флаг сработавшей проверки суммы аннитета
  bResult_flag        BOOLEAN;
  -- Вес и достоверность строки качества КИ
  nWeight             NUMBER := 0;
  nVeracity           NUMBER := 0;
  -- Тип записи для таблицы лога
  rowMatcher          L_CREDIT_HISTORY_MATCHER%ROWTYPE;
  -- Взнос По Данным Бки
  nCalc_bki_payment   NUMBER := NULL;

  -- Объявляем курсоры
  -- Курсор для выбора кандидатов ЗКИ по заявке
  CURSOR curCreditHistory
  (
    pRequest_id  NUMBER
  , pStatus_id   VARCHAR2
  )
  IS
      SELECT *
        FROM CREDIT_HISTORY#LOG
       WHERE     request_id = pRequest_id
             AND TYPE = 1
             AND status_id NOT IN (SELECT COLUMN_VALUE FROM TABLE(STRING2COLLECTION(pStatus_id, '#')))
    ORDER BY credit_history_id;

  -- Курсор для сравнения ЗКИ
  CURSOR curCreditHistoryCompare
  (
    pRequest_id         NUMBER
  , pCredit_history_id  NUMBER
  , pStatus_id          VARCHAR2
  )
  IS
      SELECT *
        FROM CREDIT_HISTORY#LOG
       WHERE     request_id = pRequest_id
             AND TYPE = 1
             AND credit_history_id != pCredit_history_id
             AND status_id NOT IN (SELECT COLUMN_VALUE FROM TABLE(STRING2COLLECTION(pStatus_id, '#')))
    ORDER BY credit_history_id;

  -- Формирование параметров строк КИ
  CURSOR curGroupDouble
  (
    pGroup_index INT
  )
  IS
    WITH base
         AS (SELECT t.*
                  , DECODE(t.status_id,  9999, 4,  99999, 3,  999, 2,  1) AS dec_prior
                  , CASE WHEN t.status_id IN (14, 17) THEN 1 ELSE 0 END AS is_own
               FROM CREDIT_HISTORY#LOG t)
    SELECT chl.*
         , FIRST_VALUE(credit_history_id) OVER (ORDER BY summa_full DESC NULLS LAST) AS is_summa_full
         , FIRST_VALUE(credit_history_id) OVER (ORDER BY credit_date_beg ASC) AS is_date_begin
         , CASE WHEN chl.mop_delay IS NOT NULL THEN 1 ELSE 0 END AS is_mop_delay
         , FIRST_VALUE(credit_history_id)
             OVER (ORDER BY is_own DESC, modification_date DESC NULLS LAST, dec_prior ASC)
             AS is_modification_date
         , FIRST_VALUE(credit_history_id) OVER (ORDER BY credit_date_end ASC) AS is_date_end
      FROM BASE chl
     WHERE chl.credit_history_id IN (SELECT COLUMN_VALUE FROM TABLE(colGroups(pGroup_index)));

  -- Курсор для вычисления наиболее информативной строки качества
  CURSOR curActualMop
  IS
      SELECT double_id, mop_delay, DECODE(bki,  9999, 4,  99999, 3,  999, 2,  1) dec_prior
        FROM (SELECT double_id, mop_delay, bki
                FROM (SELECT T.*, MAX(weight) OVER () AS w_max
                        FROM TABLE(colActualMop) t)
               WHERE weight = w_max
              UNION ALL
              SELECT double_id, mop_delay, bki
                FROM (SELECT T.*, MAX(actual) OVER () AS a_max
                        FROM TABLE(colActualMop) t)
               WHERE actual = a_max)
    ORDER BY dec_prior ASC;

  -- Расчет периода
  FUNCTION calc_period(pStop_date DATE, pStart_date DATE)
    RETURN INT
  IS
  BEGIN
    -- Считаем и возвращаем результат
    RETURN CEIL(MONTHS_BETWEEN(pStop_date, pStart_date));
  END;

  FUNCTION calc_payment(pSumma NUMBER, pPeriod NUMBER)
    RETURN NUMBER
  IS
  BEGIN
    -- Считаем и возвращаем результат
    RETURN ROUND(pSumma / pPeriod);
  END;
BEGIN
  -- == ПОЛЕТЕЛИ ВОЛОСЫ НАЗАД == --
  -- Получаем данные из массива
  nRequest_id       := PK$SCORING_VARS.FN$GET('c_request.request_id');
  nRequest_react_id := PK$SCORING_VARS.FN$GET('c_request_react.request_react_id');
  nPerson_id        := PK$SCORING_VARS.FN$GET('c_request.objects_id');
  -- Получаем настроечные параметры робота
  vRowStatus_id     := PK_SCORE.FN$GET_TUNE_VALUE('status_candidate_matcher');
  aSummaFull        := PK_SCORE.FN$GET_TUNE_VALUE('weight_summa_full');
  aDateBegin        := PK_SCORE.FN$GET_TUNE_VALUE('weight_date_beg');
  aDateEnd          := PK_SCORE.FN$GET_TUNE_VALUE('weight_date_end');
  aSummaAnn         := PK_SCORE.FN$GET_TUNE_VALUE('weight_summa_ann');
  aCurrency         := PK_SCORE.FN$GET_TUNE_VALUE('weight_currency_id');
  nBall             := TO_NUMBER(PK_SCORE.FN$GET_TUNE_VALUE('diapazon_weight'));

  -- Удаляем записи по текущей заявки и реакции
  DELETE FROM L_CREDIT_HISTORY_MATCHER
        WHERE request_id = nRequest_id AND request_react_id = nRequest_react_id;

  -- ПОГНАЛИ ПО ЗАПИСЯМ КРЕДИТНОЙ ИСТОРИИ
  FOR recCreditHistory IN curCreditHistory(nRequest_id, vRowStatus_id) LOOP
    -- Для начала очищаем переменные
    dSummaAnn                 := NULL;
    dSummaFull                := NULL;
    dCurrency                 := NULL;
    dDateBegin                := NULL;
    dDateEnd                  := NULL;
    vSplit_group              := NULL;
    nCompareResult            := NULL;
    -- Очищаем коллекции
    colDouble.DELETE;
    -- Добавляем сравниваемый ID в коллекцию дублей
    colDouble.EXTEND;
    colDouble(colDouble.LAST) := recCreditHistory.credit_history_id;

    -- Сравниваем текущую строку КИ с остальными (кроме нее самой)
    FOR recCreditHistoryCompare
      IN curCreditHistoryCompare(nRequest_id, recCreditHistory.credit_history_id, vRowStatus_id) LOOP
      -- Расчитываем дистанцию для каждого атрибута
      dSummaAnn      :=
        CASE
          WHEN ROUND(recCreditHistory.credit_payment#calc, -3) =
                 ROUND(recCreditHistoryCompare.credit_payment#calc, -3) THEN
            1
          ELSE
            0
        END;
      dSummaFull      :=
        CASE
          WHEN ROUND(recCreditHistory.summa_full, -3) = ROUND(recCreditHistoryCompare.summa_full, -3) THEN 1
          ELSE 0
        END;
      dCurrency      :=
        CASE WHEN recCreditHistory.currency_id = recCreditHistoryCompare.currency_id THEN 1 ELSE 0 END;
      dDateBegin      :=
        CASE
          WHEN TRUNC(recCreditHistory.credit_date_beg) = TRUNC(recCreditHistoryCompare.credit_date_beg) THEN
            1
          ELSE
            0
        END;
      dDateEnd      :=
        CASE
          WHEN TRUNC(recCreditHistory.credit_date_end#calc) =
                 TRUNC(recCreditHistoryCompare.credit_date_end#calc) THEN
            1
          ELSE
            0
        END;
      -- Вычисляем результат сравнения
      nCompareResult      :=
          (  (dSummaAnn * aSummaAnn)
           + (dSummaFull * aSummaFull)
           + (dCurrency * aCurrency)
           + (dDateBegin * aDateBegin)
           + (dDateEnd * aDateEnd))
        / 100;

      -- Если результат >= границе диапазона попадания в дубли
      IF nCompareResult >= nBall THEN
        -- Добавляем элемент в коллекцию дублей
        colDouble.EXTEND;
        colDouble(colDouble.LAST) := recCreditHistoryCompare.credit_history_id;
      END IF;
    END LOOP;

    -- Поднимаем флаг необходимости добавления новой группы
    bAdd_group                := TRUE;

    -- Пробегаем по существующим группам
    FOR i IN COALESCE(colGroups.FIRST, 1) .. COALESCE(colGroups.LAST, 0) LOOP
      -- Сращиваем группы между собой
      vSplit_group := colGroups(i) MULTISET INTERSECT colDouble;

      -- Проверяем попадание элементов в группу
      IF vSplit_group IS NOT EMPTY THEN
        colGroups(i) := SET(colGroups(i) MULTISET UNION colDouble);
        -- Опускаем флаг
        bAdd_group   := FALSE;
        EXIT;
      END IF;
    END LOOP;

    -- Если флаг поднят, добавляем новую группу
    IF bAdd_group THEN
      colGroups.EXTEND;
      colGroups(colGroups.LAST) := colDouble;
    END IF;

    -- Если групп еще нет, добавляем первую
    IF colGroups.COUNT = 0 THEN
      colGroups.EXTEND;
      colGroups(colGroups.LAST) := colDouble;
    END IF;
  END LOOP;

  -- Погнали по каждой группе дублей
  FOR grpIdx IN COALESCE(colGroups.FIRST, 1) .. COALESCE(colGroups.LAST, 0) LOOP
    -- Очищаем коллекции и переменные для следующей итерации
    nCnt_veb                    := 0;
    nCalc_credit_period         := NULL;
    rowMatcher                  := NULL;
    bResult_flag                := TRUE;
    colPayment.DELETE;
    colActualMop.DELETE;
    -- Определяем перемнные для логирования
    rowMatcher.person_id        := nPerson_id;
    rowMatcher.request_id       := nRequest_id;
    rowMatcher.request_react_id := nRequest_react_id;

    -- Определяем строку набора дублей
    SELECT WM_CONCAT(COLUMN_VALUE) INTO rowMatcher.group_double_id FROM TABLE(colGroups(grpIdx));

    -- Пробегаем по строкам группы
    FOR recGroupDouble IN curGroupDouble(grpIdx) LOOP
      -- == ОПРЕДЕЛЯЕМ ПАРАМЕТРЫ СТРОКИ КИ == --
      -- СУММА КРЕДИТА И ВАЛЮТА --
      IF recGroupDouble.credit_history_id = recGroupDouble.is_summa_full THEN
        rowMatcher.credit_summ_full := recGroupDouble.summa_full;
        rowMatcher.currency_id      := recGroupDouble.currency_id;
      END IF;

      -- ДАТА ОТКРЫТИЯ КРЕДИТА --
      IF recGroupDouble.credit_history_id = recGroupDouble.is_date_begin THEN
        rowMatcher.credit_date_begin := TRUNC(recGroupDouble.credit_date_beg);
      END IF;

      -- СТРОКА КАЧЕСТВА --
      IF recGroupDouble.is_mop_delay = 1 THEN
        -- Если количество записей > 1
        IF colGroups(grpIdx).COUNT > 1 THEN
          -- Расчитываем вес и достоверность, для ЗКИ с ненулевой строкой качества
          PR$CALC#WEIGHT_MOP_DELAY(recGroupDouble.mop_delay, nWeight, nVeracity);
          -- Добавляем вычисленные атрибуты в колекцию
          colActualMop.EXTEND;
          colActualMop(colActualMop.LAST)      :=
            T_O_ACTUAL_MOP(recGroupDouble.credit_history_id, recGroupDouble.mop_delay, nWeight, nVeracity
          , recGroupDouble.status_id);
        -- Если количество записей = 1
        ELSE
          -- Определяем строку качества
          rowMatcher.inf_mop_delay := recGroupDouble.mop_delay;
        END IF;
      END IF;

      -- КУЧА АТРИБУТОВ --
      IF recGroupDouble.credit_history_id = recGroupDouble.is_modification_date THEN
        rowMatcher.residual_debt           := ROUND(recGroupDouble.summa_base_dolg);
        rowMatcher.retail_product_group_id := recGroupDouble.retail_product_group_id;
        rowMatcher.delay_current           := recGroupDouble.delay_current;
        rowMatcher.summa_delay             := recGroupDouble.summa_delay;
        rowMatcher.delay_days              := recGroupDouble.delay_days;
        rowMatcher.defolt                  := recGroupDouble.defolt;
        rowMatcher.date_modify             := recGroupDouble.modification_date;
        rowMatcher.percent#calc            := recGroupDouble.percent#calc;
        rowMatcher.is_active               := recGroupDouble.is_active;
        rowMatcher.person_type             := recGroupDouble.TYPE;
        nCalc_bki_payment                  := recGroupDouble.credit_payment#calc;
      END IF;

      -- ДАТА ЗАКРЫТИЯ КРЕДИТА --
      IF recGroupDouble.credit_history_id = recGroupDouble.is_date_end THEN
        rowMatcher.credit_date_end := TRUNC(recGroupDouble.credit_date_end);
      END IF;

      -- ПРИЗНАК ПРИНАДЛЕЖНОСТИИ КРЕДИТА К ВЭБ --
      IF recGroupDouble.is_own = 1 THEN
        -- Считаем кол-во ВЭБ кредитов в группе
        nCnt_veb := nCnt_veb + 1;
      END IF;

      -- РАЗМЕР АННУИТЕТА --
      IF colGroups(grpIdx).COUNT > 1 THEN
        -- СКЛАДЫВАЕМ АТРИБУТЫ ДЛЯ РАСЧЕТА АННУИТЕТА --
        colPayment.EXTEND;
        colPayment(colPayment.LAST)      :=
          T_O_PAYMENT(recGroupDouble.credit_history_id, recGroupDouble.sum_ann, recGroupDouble.dec_prior
        , recGroupDouble.is_own);
      ELSE
        -- Определяем сумму аннуитета
        rowMatcher.credit_summ_ann := recGroupDouble.sum_ann;
      END IF;
    END LOOP;

    -- ПЫТАЕМСЯ ПОЛУЧИТЬ СУММУ АННУИТЕТА --
    IF colGroups(grpIdx).COUNT > 1 THEN
      -- Попытка номер раз
      -- Ищем взнос по кредиту ВЭБ. Сортировочка в обратном порядке ибо может быть несколько кредитов ВЭБ в группе.
      BEGIN
        SELECT DISTINCT FIRST_VALUE(payment) OVER (ORDER BY payment DESC NULLS LAST)
          INTO rowMatcher.credit_summ_ann
          FROM TABLE(colPayment)
         WHERE own = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          -- Опускаем флаг
          bResult_flag := FALSE;
      END;

      -- Если предыдущая попытка провалилась
      -- Попытка номер два
      IF NOT bResult_flag THEN
        -- Поднимаем флаг
        bResult_flag := TRUE;

        -- Пытаемся найти ненулевые взносы, повторяющие больше одного раза
        BEGIN
            SELECT DISTINCT FIRST_VALUE(payment) OVER (ORDER BY COUNT(*) DESC)
              INTO rowMatcher.credit_summ_ann
              FROM TABLE(colPayment)
             WHERE payment <> 0 AND payment IS NOT NULL
          GROUP BY payment
            HAVING COUNT(*) > 1;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- Опускаем флаг
            bResult_flag := FALSE;
        END;
      END IF;

      -- Если предыдущая попытка провалилась
      -- Попытка номер три
      IF NOT bResult_flag THEN
        -- Поднимаем флаг
        bResult_flag := TRUE;

        -- Ищем кол-во нулевых значений
        SELECT COUNT(*)
          INTO nCnt_zero
          FROM TABLE(colPayment)
         WHERE payment = 0;

        -- Если все взносы равны 0
        CASE
          WHEN colPayment.COUNT = nCnt_zero THEN
            -- Сумма аннуитета = 0
            rowMatcher.credit_summ_ann := 0;
          ELSE
            -- Опускаем флаг
            bResult_flag := FALSE;
        END CASE;
      END IF;

      -- Если предыдущая попытка провалилась
      -- Попытка номер четыре (она же последняя)
      IF NOT bResult_flag THEN
        -- Сортировочка по приоритету БКИ (исключая ВЭБ)
        BEGIN
          SELECT DISTINCT FIRST_VALUE(payment) OVER (ORDER BY priority ASC)
            INTO rowMatcher.credit_summ_ann
            FROM TABLE(colPayment)
           WHERE payment <> 0 AND priority <> 1;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            -- Опускаем флаг
            bResult_flag := FALSE;
        END;
      END IF;
    END IF;

    -- По-итогу, если флаг опущен, значит сумма аннуитета null

    -- ОПРЕДЕЛЯЕМ НАШ ЭТО КРЕДИТ ИЛИ НЕТ
    rowMatcher.is_own           := CASE nCnt_veb WHEN 0 THEN 'N' ELSE 'Y' END;

    -- Если количество записей > 1
    IF colGroups(grpIdx).COUNT > 1 THEN
      -- Вычисляем наиболее информативную строку качества с приоритетом по БКИ
      FOR recActualMop IN curActualMop LOOP
        -- Определяем строку качества
        rowMatcher.inf_mop_delay := recActualMop.mop_delay;
        -- Выходим из цикла после первой итерации
        EXIT;
      END LOOP;
    END IF;

    -- Берем кредиты БКИ
    IF rowMatcher.is_own = 'N' THEN
      -- РАСЧЕТ ВЗНОСА ПО ДАННЫМ БКИ --
      rowMatcher.calc_bki_payment := nCalc_bki_payment;

      -- РАСЧЕТ КОРРЕКТНОГО ЕВ ПО ДАННЫМ БКИ --
      -- Если куча условий
      IF     rowMatcher.credit_summ_ann <> 0
         AND rowMatcher.credit_summ_ann IS NOT NULL
         AND rowMatcher.credit_summ_ann >
               rowMatcher.calc_bki_payment / PK_SCORE.FN$GET_TUNE_VALUE('k_min_payment_bki')
         AND rowMatcher.credit_summ_ann <
               rowMatcher.calc_bki_payment * PK_SCORE.FN$GET_TUNE_VALUE('k_max_payment_bki') THEN
        -- Собственно корректный ЕВ
        rowMatcher.correct_payment := rowMatcher.credit_summ_ann;
      ELSE
        -- Собственно корректный ЕВ
        rowMatcher.correct_payment := rowMatcher.calc_bki_payment;
      END IF;

      -- ПРИЗНАК КОРРЕКТНОСТИ ПЕРЕДАННОГО ЕВ ОТ БКИ --
      rowMatcher.is_correct_summ_ann      :=
        CASE
          WHEN     rowMatcher.credit_summ_ann <> 0
               AND rowMatcher.credit_summ_ann IS NOT NULL
               AND rowMatcher.credit_summ_ann >
                     rowMatcher.calc_bki_payment / PK_SCORE.FN$GET_TUNE_VALUE('k_min_payment_bki')
               AND rowMatcher.credit_summ_ann <
                     rowMatcher.calc_bki_payment * PK_SCORE.FN$GET_TUNE_VALUE('k_max_payment_bki') THEN
            'Y'
          ELSE
            'N'
        END;
    END IF;

    -- Логируем результаты работы процедуры
    INSERT INTO L_CREDIT_HISTORY_MATCHER
         VALUES rowMatcher;
  END LOOP;

  -- Логируем КИ членов семьи
  PR#LOGGING_FAMILY_MATCHER;

  -- Фиксируем изменения в БД
  COMMIT;
END;

/
