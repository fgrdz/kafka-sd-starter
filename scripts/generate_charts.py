#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_FILE = Path("data/processed/runs.csv")
OUTPUT_DIR = Path("data/processed/charts")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_figure(fig: plt.Figure, filename: str) -> None:
    """Salva cada gráfico em PNG para slides e PDF vetorial para o relatório."""
    fig.tight_layout()
    fig.savefig(
        OUTPUT_DIR / f"{filename}.png",
        dpi=300,
        bbox_inches="tight",
    )
    fig.savefig(
        OUTPUT_DIR / f"{filename}.pdf",
        bbox_inches="tight",
    )
    plt.close(fig)


def ordered_group(
    dataframe: pd.DataFrame,
    profile: str,
    scenario: str,
) -> pd.DataFrame:
    return (
        dataframe[
            (dataframe["profile"] == profile)
            & (dataframe["scenario"] == scenario)
        ]
        .sort_values("repetition")
        .reset_index(drop=True)
    )


def chart_failed_messages(dataframe: pd.DataFrame) -> None:
    """Gráfico 1: falhas explícitas por repetição no cenário de falha."""
    a_fault = ordered_group(dataframe, "A", "fault")
    b_fault = ordered_group(dataframe, "B", "fault")

    repetitions = a_fault["repetition"].astype(int).tolist()
    positions = list(range(len(repetitions)))
    width = 0.36

    fig, ax = plt.subplots(figsize=(9, 5.5))

    ax.bar(
        [position - width / 2 for position in positions],
        a_fault["failed"],
        width=width,
        label="Perfil A",
    )

    ax.bar(
        [position + width / 2 for position in positions],
        b_fault["failed"],
        width=width,
        label="Perfil B",
    )

    ax.set_title("Falhas de produção após remoção do broker líder")
    ax.set_xlabel("Repetição")
    ax.set_ylabel("Mensagens com falha")
    ax.set_xticks(positions)
    ax.set_xticklabels(repetitions)
    ax.legend()
    ax.grid(axis="y", alpha=0.25)

    for index, value in enumerate(a_fault["failed"]):
        ax.text(
            index - width / 2,
            value,
            f"{int(value)}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    save_figure(fig, "01-falhas-producao-por-repeticao")


def chart_recovery_times(dataframe: pd.DataFrame) -> None:
    """Gráfico 2: distribuição dos tempos de recuperação de A e B."""
    fault = dataframe[dataframe["scenario"] == "fault"].copy()

    metrics = {
        "infrastructure_seconds": "Infraestrutura",
        "kafka_seconds": "Kafka",
        "application_seconds": "Aplicação",
        "performance_seconds": "Desempenho",
    }

    box_values: list[list[float]] = []
    labels: list[str] = []

    for metric, display_name in metrics.items():
        for profile in ("A", "B"):
            values = (
                fault[fault["profile"] == profile][metric]
                .dropna()
                .astype(float)
                .tolist()
            )

            box_values.append(values)
            labels.append(f"{display_name}\nPerfil {profile}")

    fig, ax = plt.subplots(figsize=(12, 6.5))

    ax.boxplot(
        box_values,
        tick_labels=labels,
        showmeans=True,
    )

    ax.set_title("Distribuição dos tempos de recuperação")
    ax.set_ylabel("Tempo após a falha (segundos)")
    ax.grid(axis="y", alpha=0.25)

    save_figure(fig, "02-tempos-recuperacao")


def chart_baseline_acknowledged(dataframe: pd.DataFrame) -> None:
    """Gráfico 3: mensagens reconhecidas nos baselines A e B."""
    a_baseline = ordered_group(dataframe, "A", "baseline")
    b_baseline = ordered_group(dataframe, "B", "baseline")

    repetitions = a_baseline["repetition"].astype(int).tolist()

    fig, ax = plt.subplots(figsize=(9, 5.5))

    ax.plot(
        repetitions,
        a_baseline["acknowledged"],
        marker="o",
        label="Perfil A",
    )

    ax.plot(
        repetitions,
        b_baseline["acknowledged"],
        marker="o",
        label="Perfil B",
    )

    ax.set_title("Mensagens reconhecidas em operação normal")
    ax.set_xlabel("Repetição")
    ax.set_ylabel("Mensagens reconhecidas")
    ax.set_xticks(repetitions)
    ax.legend()
    ax.grid(alpha=0.25)

    save_figure(fig, "03-reconhecidas-baseline")


def chart_integrity(dataframe: pd.DataFrame) -> None:
    """Gráfico 4: soma dos indicadores de integridade sob falha."""
    fault = dataframe[dataframe["scenario"] == "fault"].copy()

    metrics = [
        ("acknowledged_missing", "Reconhecidas\nausentes"),
        ("duplicates", "Duplicações"),
        ("sequence_gaps", "Ocorrências de\nlacuna"),
        ("final_lag", "Lag final"),
    ]

    a_values = [
        fault[fault["profile"] == "A"][metric].sum()
        for metric, _ in metrics
    ]

    b_values = [
        fault[fault["profile"] == "B"][metric].sum()
        for metric, _ in metrics
    ]

    labels = [label for _, label in metrics]
    positions = list(range(len(labels)))
    width = 0.36

    fig, ax = plt.subplots(figsize=(10, 5.5))

    ax.bar(
        [position - width / 2 for position in positions],
        a_values,
        width=width,
        label="Perfil A",
    )

    ax.bar(
        [position + width / 2 for position in positions],
        b_values,
        width=width,
        label="Perfil B",
    )

    ax.set_title("Indicadores de integridade nas execuções com falha")
    ax.set_xlabel("Indicador")
    ax.set_ylabel("Total nas cinco repetições")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(axis="y", alpha=0.25)

    for position, value in zip(positions, a_values):
        ax.text(
            position - width / 2,
            value,
            f"{int(value)}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    for position, value in zip(positions, b_values):
        ax.text(
            position + width / 2,
            value,
            f"{int(value)}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    save_figure(fig, "04-indicadores-integridade")


def main() -> None:
    if not INPUT_FILE.exists():
        raise SystemExit(f"Arquivo não encontrado: {INPUT_FILE}")

    dataframe = pd.read_csv(INPUT_FILE)

    required_columns = {
        "profile",
        "scenario",
        "repetition",
        "acknowledged",
        "failed",
        "acknowledged_missing",
        "duplicates",
        "sequence_gaps",
        "final_lag",
        "infrastructure_seconds",
        "kafka_seconds",
        "application_seconds",
        "performance_seconds",
    }

    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        raise SystemExit(
            "Colunas ausentes no runs.csv: "
            + ", ".join(sorted(missing_columns))
        )

    chart_failed_messages(dataframe)
    chart_recovery_times(dataframe)
    chart_baseline_acknowledged(dataframe)
    chart_integrity(dataframe)

    print(f"Gráficos gerados em: {OUTPUT_DIR.resolve()}")

    for path in sorted(OUTPUT_DIR.iterdir()):
        print(f"- {path}")


if __name__ == "__main__":
    main()
