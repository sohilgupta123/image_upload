
from PIL import Image, ImageEnhance, ImageFile
from boto3.session import Session
from kafka import KafkaConsumer
import traceback
import subprocess
import boto3
import glob
import json
import PIL
import pdb
import re
import os

BUCKET = 'qas14'
PROJECT_DIR = os.path.abspath(os.path.dirname(__file__).decode('utf-8'))
MEDIA_ROOT = os.path.join(PROJECT_DIR, 'media')
PRODUCT_IMAGES_ROOT = os.path.join(MEDIA_ROOT, 'uploads', 'p')
PRODUCT_NON_WM_IMAGES_ROOT = os.path.join(MEDIA_ROOT, 'uploads', 'pnw')
UPLOAD_DIR = os.path.join(MEDIA_ROOT, 'uploads')
PATH = os.path.join(UPLOAD_DIR, 'image_unzipped')
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


class S3Boto3Util(object):

    def __init__(self, bucket_name=''):
        """
        :param bucket_name: str
        """
        import boto3
        from boto3 import client
        self.s3_resource = boto3.resource('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        # cur_meta_data['staged_delete'] = 'true'
        self.client = client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        self.bucket_name = bucket_name

    def replace_and_add_tag(self, key_name, tags, metadata=None, delete_old_version=False):
        """
        :param key_name: str
        :param tags: str. Tags must be like URL encoded variables. Ex: 'delete=true'
        :param metadata: dict
        :param delete_old_version: bool
        :return:
        """
        bucket_name = self.bucket_name
        t_object = self.s3_resource.Object(bucket_name, key_name)
        logger.debug("replace_and_add_tag started for, bucket_name {}, key {}".format(
            bucket_name, key_name))
        try:
            t_object.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info("replace_and_add_tag in bucket {}, key {} not found so skipping to next".format(bucket_name,
                                                                                                            key_name))
                return True
            else:
                raise

        cur_metadata = t_object.metadata or {}
        old_version_id = t_object.version_id
        if metadata:
            cur_metadata.update(metadata)

        resp = self.client.copy_object(Bucket=bucket_name, Key=key_name,
                                       CopySource={
                                           'Bucket': bucket_name, 'Key': key_name},
                                       Metadata=cur_metadata, MetadataDirective='REPLACE',
                                       Tagging=tags, TaggingDirective='REPLACE')
        status = False
        new_version_id = None
        if not (resp and resp.get('ResponseMetadata', {})):
            logger.error("Error while copying+ tagging key {} in bucket {} response: {}".format(key_name, bucket_name,
                                                                                                resp))
        else:
            status = True
            new_version_id = resp.get('VersionId')
            if not new_version_id:
                logger.warning("Got no version id for key {} , bucket {}".format(
                    key_name, bucket_name))

        del_stat = "Not Applicable"
        if delete_old_version:
            del_stat = True
            del_response = self.client.delete_object(
                Bucket=bucket_name, Key=key_name, VersionId=old_version_id)
            if not (del_response and del_response.get('ResponseMetadata', {})):
                del_stat = False
                logger.error("Error while Deleting key {} version {} in bucket {} response: {}".format(
                    key_name, old_version_id, bucket_name, resp))

        logger.info("replace_and_add_tag: making copy of object key {} s3 bucket {}. status {}, old version id {}, "
                    "new version id {}, delete status {}".format(key_name, bucket_name, status, old_version_id,
                                                                 new_version_id, del_stat))

        return status


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
        # cmd = "s3cmd sync %s %s" % (s3bucket, local_path)
        subprocess.call(["s3cmd", "sync", s3bucket, local_path])
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # logger.debug(u"Downloaded Water Mark images : %s" % cmd)

    def reduce_opacity(self, im, opacity):
        """Returns an image with reduced opacity."""
        assert 0 <= opacity <= 1
        if im.mode != 'RGBA':
            im = im.convert('RGBA')
        else:
            im = im.copy()
        alpha = im.split()[3]
        alpha = ImageEnhance.Brightness(alpha).enhance(opacity)
        im.putalpha(alpha)
        return im

    def watermark(self, fl, im, mark, position, opacity=1):
        """Adds a watermark to an image."""
        if opacity < 1:
            mark = self.reduce_opacity(mark, opacity)
        if im.mode != 'RGBA':
            im = im.convert('RGBA')
            # create a transparent layer the size of the image and draw the
        # watermark in that layer.
        layer = Image.new('RGBA', im.size, (0, 0, 0, 0))
        if position == 'tile':
            for y in range(0, im.size[1], mark.size[1]):
                for x in range(0, im.size[0], mark.size[0]):
                    layer.paste(mark, (x, y))
        elif position == 'scale':
            # scale, but preserve the aspect ratio
            ratio = min(
                float(im.size[0]) / mark.size[0], float(im.size[1]) / mark.size[1])
            w = int(mark.size[0] * ratio)
            h = int(mark.size[1] * ratio)
            mark = mark.resize((w, h))
            layer.paste(mark, ((im.size[0] - w) / 2, (im.size[1] - h) / 2))
        else:
            layer.paste(mark, position)
            # composite the watermark with the layer
        return Image.composite(layer, im, layer)

    def upload_to_s3_media(self, bucket, local_file, base_file, ver_name, size, key_dir, city=None, bundle_image=False, sku_id=None):
        # check_image_and_compress(local_file, sku_id)
        key = Key(bucket)
        logger.debug("city:%s and bundle_image:%s" % (city, bundle_image))
        if bundle_image:
            key.key = 'media/uploads/bpi/%s/%s/%s' % (city, size, ver_name)
        else:
            key.key = 'media/uploads/%s/%s/%s' % (key_dir, size, ver_name)
        key.set_contents_from_filename(local_file)
        logger.debug(u"Uploading product image %s into S3 Bucket" % key.key)
        return key.key

    def upload_to_s3_non_watermark(self, bucket, local_file, base_file, ver_name, size, key_dir, city=None, bundle_image=False, sku_id=None):
        # check_image_and_compress(local_file,sku_id)
        key = Key(bucket)
        if bundle_image:
            key.key = 'media/uploads/bpi/%s/%s/%s' % (city, size, ver_name)
        else:
            key.key = 'media/uploads/%s/%s/%s' % (key_dir, size, ver_name)
        key.set_contents_from_filename(local_file)
        logger.debug(
            u"Uploading product versioned image %s into S3 Bucket" % key.key)
        return key.key

    def tag_previous_version_for_deletion(self, s3_keys_uploaded, new_name, old_name, new_no_wm_name, old_no_wm_name,
                                          bucket_name):
        """
        :param s3_keys_uploaded: list
        :param new_name: str, file name as per new version
        :param old_name: str, old file name as per new version
        :param new_no_wm_name: file name of without water mark as per new version
        :param old_no_wm_name: old file name as per new version
        :param bucket_name: str
        :return:
        """
        keys_to_tag = [x.replace(new_name, old_name).replace(
            new_no_wm_name, old_no_wm_name) for x in s3_keys_uploaded]
        s3util = S3Boto3Util(bucket_name)
        status = True
        for key in keys_to_tag:
            status &= s3util.replace_and_add_tag(
                key, 'delete=true', delete_old_version=True)
        logger.info("Added tags to total {} keys, status{}".format(
            len(keys_to_tag), status))
        return status

    def resize_images(self, sku_id, version, infile, files_path, save_files=None):

        resize_dir = PRODUCT_IMAGES_ROOT
        nw_resize_dir = PRODUCT_NON_WM_IMAGES_ROOT

        # Make Directory for different sizes of images.
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

        all_images = SortedDict()

        try:
            prod = None
            xl_exist = False
            # In 900x900 we will use the original image without any conversion
            temp = infile.replace('.png', '.jpg').replace('.PNG', '.jpg')
            os.rename(os.path.join(files_path, infile),
                      os.path.join(files_path, temp))
            infile = temp
            base_file = os.path.basename(infile)
            print("Processing file:%s" % infile)
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

            if '1' in (_file.split('-')):
                _file = _file.split('-')[0]
            print("Uploaded product Image format is : %s" % img.format)

            if img_png.format not in ['JPEG', 'PNG']:
                msg = 'Image format is not JPEG or PNG. It is %s' % img.format
                all_images[str(base_file).split('.')[0]] = [
                    base_file, "Failed", msg]
                return None

            # image_number = str(base_file.split('.')[0]).split('-')[1]
            # if image_number == '1':
            #     version = prod.version + 1
            # else:
            #     if prod.images_meta_data:
            #         pd_images_meta = json.loads(prod.images_meta_data)
            #         try:
            #             version = int(
            #                 pd_images_meta[str(image_number)]['version']) + 1
            #         except KeyError:
            #             logger.info(
            #                 "Version key for %s is not found" % prod)
            #             version = 1
            #     else:
            #         version = 1
            #         logger.info("No meta data for product %s for image number %s" % (
            #             prod, str(image_number)))

            # logger.info("New version for %d and image number %s is %d" % (
            #     prod.id, image_number, version))

            # block to form new / old file names based on version

            # incrementing version
            version = version + 1

            new_name = "%s_%s%s" % (_file, version, ext)
            old_name = "%s_%s%s" % (_file, version - 1, ext)

            no_wm_name = "%s_%s%s" % (self.generate_raw_image_filename(
                _file), version, ext)  # Non watermarked images

            old_no_watermark_name = "%s_%s%s" % (self.generate_raw_image_filename(
                _file), version - 1, ext)  # Non watermarked images
            print("new name %s no wm name %s" % (new_name, no_wm_name))

            xs_img = img.resize((50, 50), Image.ANTIALIAS)
            xs_no_wm_img = img.resize((50, 50), Image.ANTIALIAS)
            xs_wm_img = os.path.join(wm_dir, 'watermark_1608_xs.png')

            if os.path.exists(xs_wm_img):
                xs_mark = PIL.Image.open(xs_wm_img)
                xs_img = self.watermark(
                    infile, xs_img, xs_mark, (25, 25), 1.5)
                print("Creating Water Mark 'XS' type image")

            s_img = img.resize((150, 150), Image.ANTIALIAS)
            s_no_wm_img = img.resize((150, 150), Image.ANTIALIAS)
            s_wm_img = os.path.join(wm_dir, 'watermark_1608_s.png')
            if os.path.exists(s_wm_img):
                s_mark = PIL.Image.open(s_wm_img)
                s_img = watermark(infile, s_img, s_mark, (75, 75), 1.5)
                print("Creating Water Mark 'S' type image")

            m_img = img.resize((300, 300), Image.ANTIALIAS)
            m_no_wm_img = img.resize((300, 300), Image.ANTIALIAS)
            m_wm_img = os.path.join(wm_dir, 'watermark_1608_m.png')
            if os.path.exists(m_wm_img):
                m_mark = PIL.Image.open(m_wm_img)
                m_img = watermark(infile, m_img, m_mark, (150, 150), 1.5)
                print("Creating Water Mark 'M' type image")

            l_img = img.resize((500, 500), Image.ANTIALIAS)
            l_no_wm_img = img.resize((500, 500), Image.ANTIALIAS)
            l_wm_img = os.path.join(wm_dir, 'watermark_1608_l.png')
            if os.path.exists(l_wm_img):
                l_mark = PIL.Image.open(l_wm_img)
                l_img = watermark(infile, l_img, l_mark, (250, 250), 1.5)
                print("Creating Water Mark 'L' type image")

            if img_png.size == (900, 900) and img_png.format == 'PNG':
                xl_img = img.resize((600, 600), Image.ANTIALIAS)
                xl_no_wm_img = img.resize((600, 600), Image.ANTIALIAS)
                print("Creating 'XL' type image")

                # Copying the same png image in watermark as well as in non watermarked images
                # We have just renamed the .png to .jpg and the format is still 'PNG'
                xxl_img = img_png
                xxl_no_wm_img = img_png

                xl_exist = True
                print("Creating 'XXL' type image")

            ml_img = img.resize((190, 190), Image.ANTIALIAS)
            ml_no_wm_img = img.resize((190, 190), Image.ANTIALIAS)
            ml_wm_img = os.path.join(wm_dir, 'watermark_1608_ml.png')
            if os.path.exists(ml_wm_img):
                ml_mark = PIL.Image.open(ml_wm_img)
                ml_img = self.watermark(
                    infile, ml_img, ml_mark, (95, 95), 1.5)
                print("Creating Water Mark 'ML' type image")

            mm_img = img.resize((125, 125), Image.ANTIALIAS)
            mm_no_wm_img = img.resize((125, 125), Image.ANTIALIAS)
            mm_wm_img = os.path.join(wm_dir, 'watermark_1608_mm.png')
            if os.path.exists(mm_wm_img):
                mm_mark = PIL.Image.open(mm_wm_img)
                mm_img = self.watermark(
                    infile, mm_img, mm_mark, (60, 60), 1.5)
                print("Creating Water Mark 'MM' type image")

            xs_file = os.path.join(xs_dir, base_file)
            xs_no_wm_file = os.path.join(xs_dir_nw, no_wm_base_file)
            xs_img.save(xs_file, "JPEG", quality=80,
                        optimize=True, progressive=True)
            xs_no_wm_img.save(xs_no_wm_file, "JPEG",
                              quality=80, optimize=True, progressive=True)

            s_file = os.path.join(s_dir, base_file)
            s_no_wm_file = os.path.join(s_dir_nw, no_wm_base_file)
            s_img.save(s_file, "JPEG", quality=80,
                       optimize=True, progressive=True)
            s_no_wm_img.save(s_no_wm_file, "JPEG", quality=80,
                             optimize=True, progressive=True)

            m_file = os.path.join(m_dir, base_file)
            m_no_wm_file = os.path.join(m_dir_nw, no_wm_base_file)
            m_img.save(m_file, "JPEG", quality=80,
                       optimize=True, progressive=True)
            m_no_wm_img.save(m_no_wm_file, "JPEG", quality=80,
                             optimize=True, progressive=True)

            l_file = os.path.join(l_dir, base_file)
            l_no_wm_file = os.path.join(l_dir_nw, no_wm_base_file)
            l_img.save(l_file, "JPEG", quality=80,
                       optimize=True, progressive=True)
            l_no_wm_img.save(l_no_wm_file, "JPEG", quality=80,
                             optimize=True, progressive=True)

            if xl_exist:
                xl_file = os.path.join(xl_dir, base_file)
                xl_no_wm_file = os.path.join(xl_dir_nw, no_wm_base_file)
                xl_img.save(xl_file, "JPEG", quality=80,
                            optimize=True, progressive=True)
                xl_no_wm_img.save(
                    xl_no_wm_file, "JPEG", quality=80, optimize=True, progressive=True)

                xxl_file = os.path.join(xxl_dir, base_file)
                xxl_no_wm_file = os.path.join(xxl_dir_nw, no_wm_base_file)
                xxl_img.save(xxl_file, "PNG", quality=80,
                             optimize=True, progressive=True)
                xxl_no_wm_img.save(
                    xxl_no_wm_file, "PNG", quality=80, optimize=True, progressive=True)

            ml_file = os.path.join(ml_dir, base_file)
            ml_no_wm_file = os.path.join(ml_dir_nw, no_wm_base_file)
            ml_img.save(ml_file, "JPEG", quality=80,
                        optimize=True, progressive=True)
            ml_no_wm_img.save(ml_no_wm_file, "JPEG",
                              quality=80, optimize=True, progressive=True)

            mm_file = os.path.join(mm_dir, base_file)
            mm_no_wm_file = os.path.join(mm_dir_nw, no_wm_base_file)
            mm_img.save(mm_file, "JPEG", quality=80,
                        optimize=True, progressive=True)
            mm_no_wm_img.save(mm_no_wm_file, "JPEG",
                              quality=80, optimize=True, progressive=True)
            successfully_uploaded_next_version_to_s3 = False

            s3_keys_uploaded = []

            if s3:
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, xs_file, base_file, new_name, 'xs', 'p', sku_id=sku_id))
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, s_file, base_file, new_name, 's', 'p', sku_id=sku_id))
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, m_file, base_file, new_name, 'm', 'p', sku_id=sku_id))
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, l_file, base_file, new_name, 'l', 'p', sku_id=sku_id))
                if xl_exist:
                    s3_keys_uploaded.append(self.upload_to_s3_media(
                        bucket, xl_file, base_file, new_name, 'xl', 'p', sku_id=sku_id))
                    s3_keys_uploaded.append(self.upload_to_s3_media(
                        bucket, xxl_file, base_file, new_name, 'xxl', 'p', sku_id=sku_id))
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, ml_file, base_file, new_name, 'ml', 'p', sku_id=sku_id))
                s3_keys_uploaded.append(self.upload_to_s3_media(
                    bucket, mm_file, base_file, new_name, 'mm', 'p', sku_id=sku_id))

                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, xs_no_wm_file, base_file, no_wm_name, 'xs', 'pnw', sku_id=sku_id))
                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, s_no_wm_file, base_file, no_wm_name, 's', 'pnw', sku_id=sku_id))
                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, m_no_wm_file, base_file, no_wm_name, 'm', 'pnw', sku_id=sku_id))
                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, l_no_wm_file, base_file, no_wm_name, 'l', 'pnw', sku_id=sku_id))
                if xl_exist:
                    s3_keys_uploaded.append(
                        self.upload_to_s3_non_watermark(bucket, xl_no_wm_file, base_file, no_wm_name, 'xl', 'pnw', sku_id=sku_id))
                    s3_keys_uploaded.append(
                        self.upload_to_s3_non_watermark(bucket, xxl_no_wm_file, base_file, no_wm_name, 'xxl', 'pnw', sku_id=sku_id))
                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, ml_no_wm_file, base_file, no_wm_name, 'ml', 'pnw',
                                                    sku_id=sku_id))
                s3_keys_uploaded.append(
                    self.upload_to_s3_non_watermark(bucket, mm_no_wm_file, base_file, no_wm_name, 'mm', 'pnw',
                                                    sku_id=sku_id))
                successfully_uploaded_next_version_to_s3 = True
            else:
                xs_img.save(os.path.join(xs_dir, new_name), "JPEG",
                            quality=80, optimize=True, progressive=True)
                s_img.save(os.path.join(s_dir, new_name), "JPEG",
                           quality=80, optimize=True, progressive=True)
                m_img.save(os.path.join(m_dir, new_name), "JPEG",
                           quality=80, optimize=True, progressive=True)
                l_img.save(os.path.join(l_dir, new_name), "JPEG",
                           quality=80, optimize=True, progressive=True)
                if xl_exist:
                    xl_img.save(os.path.join(xl_dir, new_name), "JPEG",
                                quality=80, optimize=True, progressive=True)
                    xxl_img.save(os.path.join(xxl_dir, new_name), "PNG",
                                 quality=80, optimize=True, progressive=True)
                ml_img.save(os.path.join(ml_dir, new_name), "JPEG",
                            quality=80, optimize=True, progressive=True)
                mm_img.save(os.path.join(mm_dir, new_name), "JPEG",
                            quality=80, optimize=True, progressive=True)

                xs_no_wm_img.save(os.path.join(
                    xs_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                s_no_wm_img.save(os.path.join(
                    s_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                m_no_wm_img.save(os.path.join(
                    m_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                l_no_wm_img.save(os.path.join(
                    l_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                if xl_exist:
                    xl_no_wm_img.save(os.path.join(
                        xl_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                    xxl_no_wm_img.save(os.path.join(
                        xxl_dir_nw, no_wm_name), "PNG", quality=80, optimize=True, progressive=True)
                ml_no_wm_img.save(os.path.join(
                    ml_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)
                mm_no_wm_img.save(os.path.join(
                    mm_dir_nw, no_wm_name), "JPEG", quality=80, optimize=True, progressive=True)

            resized_imgs += 1

            if successfully_uploaded_next_version_to_s3 and version > 1:
                try:
                    self.tag_previous_version_for_deletion(s3_keys_uploaded, new_name, old_name, no_wm_name,
                                                      old_no_watermark_name, bucket_name)
                except:
                    import traceback
                    fmt_msg = traceback.format_exc()
                    # Sending mail with exception.
                    # send_bbmail("exception in tagging s3 image",
                    #             fmt_msg, ['vibhore@bigbasket.com'])
                    logger.exception("exception in tagging s3 image")

            all_images[str(base_file).split('.')[0]] = [
                base_file, "Success", '']

        except Exception as e:
            logger.exception(
                "Uncaught exception in resize_images, continuing : %s " % str(e))
            logger.debug(
                "*"*50+"Uncaught exception in resize_images, continuing : %s " % str(e))
            if prod:
                all_images[str(base_file).split('.')[0]] = [
                    base_file, "Failed", "Error in upload"]
            else:
                all_images[str(base_file).split('.')[0]] = [
                    base_file, "Failed", "Error in upload"]
        return resized_imgs, all_images

consumer = KafkaConsumer('Topic', bootstrap_servers='localhost:9092')
for msg in consumer:
    # receive message
    try:
        sku_details = json.loads(msg.value)
        sku = sku_details.get("sku_id")
        path = sku_details.get("file_path")
        image_json = sku_details.get("image_json")

        images = []

        for key, val in image_json.iteritems():
            ResizeImages().resize_images(sku, val.v, val.file, path)

    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        print('Upload Execution Completed!')

    # upload images to s3
    # commit message
    # try catch
    # send respone to api.
