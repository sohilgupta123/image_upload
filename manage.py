from kafka import KafkaConsumer
from boto3.session import Session
import traceback
import boto3
import glob
import json
import PIL
import pdb
import os

bucket = 'qas14'
AWS_MEDIA_STORAGE_BUCKET_NAME = 'bbtest1'


class S3:
    def __init__(self):
        aws_access_key_id = ''
        aws_secret_access_key = ''
        self.session = Session(aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)
        self.s3 = self.session.resource('s3')
        self.s3_client = boto3.client('s3')
        print(self.s3)

    def download(self, bucket, path, f):
        return self.s3.meta.client.download_file(bucket, path, f)


class ResizeImages:
    def __init__(self):
        pass

    def create_directory_for_resize_images(self, resize_dir, nw_resize_dir):
        logger.info("Creating directory for resized Images")
        xs_dir = os.path.join(resize_dir, 'xs')
        s_dir = os.path.join(resize_dir, 's')
        m_dir = os.path.join(resize_dir, 'm')
        l_dir = os.path.join(resize_dir, 'l')
        xl_dir = os.path.join(resize_dir, 'xl')
        xxl_dir = os.path.join(resize_dir, 'xxl')
        ml_dir = os.path.join(resize_dir, 'ml')
        mm_dir = os.path.join(resize_dir, 'mm')

        # watermark directory
        wm_dir = os.path.join(resize_dir, 'wm/')

        xs_dir_nw = os.path.join(nw_resize_dir, 'xs')
        s_dir_nw = os.path.join(nw_resize_dir, 's')
        m_dir_nw = os.path.join(nw_resize_dir, 'm')
        l_dir_nw = os.path.join(nw_resize_dir, 'l')
        xl_dir_nw = os.path.join(nw_resize_dir, 'xl')
        xxl_dir_nw = os.path.join(nw_resize_dir, 'xxl')
        ml_dir_nw = os.path.join(nw_resize_dir, 'ml')
        mm_dir_nw = os.path.join(nw_resize_dir, 'mm')

        return xs_dir, s_dir, m_dir, l_dir, xl_dir, xxl_dir, ml_dir, mm_dir, wm_dir, xs_dir_nw, s_dir_nw, m_dir_nw, l_dir_nw, \
            xl_dir_nw, xxl_dir_nw, ml_dir_nw, mm_dir_nw

    def get_or_create_dir(self, dir_name):
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
            return True
        return False

    def group_sku_images(self, prod_imgs):
        # group image files sku wise
        prd_dict = {}
        for img in prod_imgs:
            if img.split('-')[0] in prd_dict:
                contents = prd_dict[img.split('-')[0]]
                contents.append(img)
                prd_dict[img.split('-')[0]] = contents
            else:
                prd_dict[img.split('-')[0]] = [img]
        return prd_dict

    def check_sku_set_validity(self, prd_dict):
        # check image files for primary image
        accept_list = []
        reject_list = []
        prod_imgs = []
        for k, img in prd_dict.iteritems():
            if str(k)+"-1.jpg" in img or str(k)+"-1.png" in img:
                for img_name in img:
                    file_name = img_name.replace(
                        '.jpg', '').replace('.png', '')
                    if len(file_name.split('-')) == 2:
                        # no type in the file name
                        sku, num = file_name.split('-')
                        if str(sku)+"-"+str(num) in prod_imgs:
                            reject_list.append(img_name)
                        else:
                            accept_list.append(img_name)
                            prod_imgs.append(str(sku)+"-"+str(num))
                    elif len(file_name.split('-')) == 3:
                        sku, num, imgtype = file_name.split('-')
                        if imgtype in ['r', 'n', 's'] and (str(sku) + "-" + str(num) not in prod_imgs):
                            accept_list.append(img_name)
                            prod_imgs.append(str(sku)+"-"+str(num))
                        else:
                            reject_list.append(img_name)
                    else:
                        reject_list.extend(img_name)
            else:
                reject_list.extend(img)

        return accept_list, reject_list

    def encode_int(self, val, key='0xbbc00cee'):
        val = int(val) ^ int(key, 16)
        return base64.b64encode(str(val))

    def generate_raw_image_filename(self, base_file):
        # return "p%s%s" % (str(product_id)[:3], encode_int(product_id))
        # return "%s%s" % (str(product_id)[:3], hex(int(product_id)).rstrip("L").lstrip("0x") or "0")
        # logger.info("base file %s" %base_file)
        base_file = str(base_file)
        product_id = base_file.split('-')[0]
        if len(base_file.split('-')) == 1:
            return self.encode_int(product_id).replace('=', '')
        if len(base_file.split('-')) == 2:
            ext = base_file.split('-')[1]
            return self.encode_int(product_id).replace('=', '')+'-'+str(ext)
        if len(base_file.split('-')) == 3:
            ext = base_file.split('-')[1]+'-'+base_file.split('-')[2]
            return self.encode_int(product_id).replace('=', '')+'-'+str(ext)

    def get_valid_images(self, files):
        prod_imgs = [os.path.basename(filename).replace(
            "JPG", "jpg").replace("PNG", "jpg") for filename in files]
        prd_dict = self.group_sku_images(prod_imgs)

        accept_list, reject_list = self.check_sku_set_validity(prd_dict)

        if accept_list:
            for val in reject_list:
                key = val.replace('.jpg', '').replace('.png', '').split('-')[0]
                if key in prd_dict:
                    del prd_dict[key]
            file_list = [filename for filename in files if
                         os.path.basename(filename).replace('JPG', 'jpg').replace('PNG', 'jpg') in accept_list]
        else:
            file_list = []

        return file_list, reject_list

    def download_water_mark_images(self, s3bucket, local_path):
        cmd = "s3cmd sync %s %s" % (s3bucket, local_path)
        runcmd(cmd)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # logger.debug(u"Downloaded Water Mark images : %s" % cmd)

    def resize_images(self, files_path, resize_dir, nw_resize_dir, save_files=None):
        xs_dir, s_dir, m_dir, l_dir, xl_dir, xxl_dir, ml_dir, mm_dir, wm_dir, \
            xs_dir_nw, s_dir_nw, m_dir_nw, l_dir_nw, xl_dir_nw, xxl_dir_nw, ml_dir_nw, mm_dir_nw = self.create_directory_for_resize_images(
                resize_dir, nw_resize_dir)

        all_dirs = [xs_dir, s_dir, m_dir, l_dir, xl_dir, xxl_dir, ml_dir, mm_dir, wm_dir, xs_dir_nw, s_dir_nw,
                    m_dir_nw, l_dir_nw, xl_dir_nw, xxl_dir_nw, ml_dir_nw, mm_dir_nw]

        for x in all_dirs:
            self.get_or_create_dir(x)

        resized_imgs = 0
        s3 = getattr(settings, 'MEDIA_FROM_S3', None)
        conn = get_s3_connection()
        bucket_name = AWS_MEDIA_STORAGE_BUCKET_NAME
        bucket = conn.get_bucket(bucket_name)
        self.download_water_mark_images(
            "s3://%s/media/uploads/p/wm/" % AWS_MEDIA_STORAGE_BUCKET_NAME,
            wm_dir
        )
        files = glob.glob(files_path + '/*.*')
        all_images = SortedDict()
        rejected_files = []

        if save_files is None:
            save_files = []
            for file_name in files:
                filename = os.path.basename(file_name).split('.')[0]
                if re.match(r'(\d+)(-)+(\d{1})(-)*(r|n)*', filename):
                    save_files.append(file_name)
                else:
                    rejected_files.append(file_name)
        if save_files:
            valid_files, invalid_files = self.get_valid_images(save_files)
            logger.info("valid %s invalid %s" % (valid_files, invalid_files))
        else:
            for infile in rejected_files:
                base_file = os.path.basename(infile).split('.')[0]
                all_images[str(base_file)] = [base_file, "Failed",
                                              "File Name is not of the right format"]
            return resized_imgs, all_images

        for infile in invalid_files:
            base_file = os.path.basename(infile).split('.')[0]
            all_images[str(base_file)] = [base_file,
                                          "Failed", "SKU set is invalid"]

        for infile in rejected_files:
            base_file = os.path.basename(infile).split('.')[0]
            all_images[str(base_file)] = [base_file, "Failed",
                                          "File Name is not of the right Format"]

        for infile in valid_files:
            try:
                prod = None
                xl_exist = False
                # In 900x900 we will use the original image without any conversion
                temp = infile.replace('.png', '.jpg').replace('.PNG', '.jpg')
                os.rename(os.path.join(files_path, infile),
                          os.path.join(files_path, temp))
                infile = temp
                base_file = os.path.basename(infile)
                logger.info("Processing file:%s" % infile)
                no_wm_base_file = "%s.%s" % (self.generate_raw_image_filename(
                    base_file.split('.')[0]), base_file.split('.')[1])
                img_png = PIL.Image.open(os.path.join(files_path, base_file))
                img = img_png

                if img.format == 'PNG':
                    bg = PIL.Image.new('RGB', img.size, (255, 255, 255))
                    bg.paste(img, (0, 0), img)
                    img = bg

                if img.size[0] and img.size[1]:
                    PIL.ImageFile.MAXBLOCK = img.size[0] * img.size[1]
                _file, ext = os.path.splitext(base_file)
                img_file_name = _file
                sku_id = _file.split('-')[0]

                #Get Product decscription here, to validate if product is present.
                # try:
                #     prod = ProductDescription.objects.get(
                #         id=int(_file.split('-')[0]))
                # except ProductDescription.DoesNotExist:
                #     logger.info("Product Description with id %d does not exist " % int(
                #         _file.split('-')[0]))
                #     all_images[str(base_file.split('.')[0])] = [
                #         base_file, "Failed", "Product Description doesnt exist"]
                #     continue


consumer = KafkaConsumer('Topic', bootstrap_servers='localhost:9092')
for msg in consumer:
    # receive message
    try:
        sku_details = json.loads(msg.value)
        sku = sku_details.get("sku")
        path = sku_details.get("path")
        file_name = path.split('/')[-1]

        # download file from s3 for given sku
        S3().download(bucket, path, 'temp/'+file_name)

        # resize images

    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        print('Upload Execution Completed!')

    # upload images to s3
    # commit message
    # try catch
    # send respone to api.
