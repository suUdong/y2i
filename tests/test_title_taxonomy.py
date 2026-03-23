from omx_brainstorm.title_taxonomy import classify_title, summarize_title_classes


def test_classify_title_recognizes_macro():
    labels = classify_title("FOMC 이후 금리 전망과 환율 방향")
    assert "매크로" in labels


def test_classify_title_recognizes_industry():
    labels = classify_title("반도체 슈퍼사이클과 2차전지 전망")
    assert "산업분석" in labels


def test_summarize_title_classes_counts_labels():
    summary = summarize_title_classes(["반도체 전망", "금리 전망"])
    assert summary["산업분석"] >= 1
    assert summary["매크로"] >= 1
