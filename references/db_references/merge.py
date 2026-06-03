import glob
import json
import os

OUTPUT_FILE = "ket_qua_tong.json"


def gop_file_json():
    du_lieu_tong = {}

    # LẤY ĐƯỜNG DẪN TUYỆT ĐỐI của thư mục chứa file merge.py này
    thu_muc_hien_tai = os.path.dirname(os.path.abspath(__file__))

    # Tạo đường dẫn tìm kiếm dạng: c:\...\TMDb_relica\*.json
    duong_dan_tim_kiem = os.path.join(thu_muc_hien_tai, "*.json")
    cac_file_json = glob.glob(duong_dan_tim_kiem)

    # Đường dẫn tuyệt đối của file output để loại trừ
    duong_dan_output = os.path.join(thu_muc_hien_tai, OUTPUT_FILE)
    if duong_dan_output in cac_file_json:
        cac_file_json.remove(duong_dan_output)

    if not cac_file_json:
        # Đã đổi sang không dấu để tránh lỗi CP1252
        print(
            f"Khong tim thay file JSON nao tai thu muc: {thu_muc_hien_tai}"
        )
        return

    print(f"Bat dau doc va gop {len(cac_file_json)} file JSON...")

    for duong_dan_file in cac_file_json:
        ten_file = os.path.basename(duong_dan_file)
        try:
            with open(duong_dan_file, "r", encoding="utf-8") as f:
                noi_dung = json.load(f)
                du_lieu_tong[ten_file] = noi_dung
                print(f"-> Da doc xong: {ten_file}")
        except json.JSONDecodeError:
            print(
                f"Loi dinh dang: File {ten_file} khong dung chuan JSON. Da bo qua."
            )
        except Exception as e:
            print(f"Loi khi doc file {ten_file}: {e}")

    # Ghi file ra ngay tại thư mục chứa code
    with open(duong_dan_output, "w", encoding="utf-8") as f_out:
        json.dump(du_lieu_tong, f_out, ensure_ascii=False, indent=8)

    print(f"\n[Thanh cong] Du lieu da duoc ghi vao: {duong_dan_output}")


if __name__ == "__main__":
    gop_file_json()