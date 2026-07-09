from hydra.utils.config import load_config


def main() -> None:
    cfg = load_config()
    live_allowed = cfg["project"].get("live_trading_allowed", False)
    print(f"HYDRA research mode. live_trading_allowed={live_allowed}")
    print("Use scripts/run_strategy_factory_v3_expansion.py for smoke research runs.")


if __name__ == "__main__":
    main()
