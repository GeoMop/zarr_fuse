import sys
from ingress_server.worker import _process_one, _iter_accepted_files_in_dir

def main():
    accepted_dir = sys.argv[1]
    for data_path in _iter_accepted_files_in_dir(accepted_dir):
        err = _process_one(data_path)
        if err:
            print(f"Error processing {data_path}: {err}")
        else:
            print(f"Successfully processed {data_path}")

if __name__ == "__main__":
    main()
